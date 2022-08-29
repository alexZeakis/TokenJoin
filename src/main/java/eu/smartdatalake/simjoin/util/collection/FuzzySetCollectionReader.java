package eu.smartdatalake.simjoin.util.collection;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.simple.JSONObject;

import gnu.trove.iterator.TIntObjectIterator;
import gnu.trove.list.TIntList;
import gnu.trove.list.array.TIntArrayList;
import gnu.trove.map.TIntObjectMap;
import gnu.trove.map.TObjectIntMap;
import gnu.trove.map.hash.TIntObjectHashMap;
import gnu.trove.map.hash.TObjectIntHashMap;
import gnu.trove.set.TIntSet;
import gnu.trove.set.hash.TIntHashSet;

/**
 * Class for loading data from a CSV file with the corresponding config JSON
 * file.
 *
 */
public class FuzzySetCollectionReader {
	protected static final Logger logger = LogManager.getLogger(FuzzySetCollectionReader.class);
	protected static long over400 = 0;
	protected static final int overLimit = 400;

	/**
	 * @param readConfig:      Config (JSON) file of the selected source file
	 *                         containing the correspondent information.
	 * @param execConfig:      Config (JSON) file of the corresponding execution.
	 * @param keepOriginal:    Boolean option for keeping original strings.
	 *                         Necessary for Edit Similarity, Optional for Jaccard
	 * @param cleanDuplicates: Boolean option to clean duplicate records.
	 * @return {@link FuzzyIntSetCollection FuzzyIntSetCollection} collection
	 */
	public FuzzyIntSetCollection prepareCollection(JSONObject readConfig, JSONObject execConfig, boolean keepOriginal,
			boolean cleanDuplicates) {
		RawCollection collection = fromCSV(readConfig, execConfig, keepOriginal);

		TObjectIntMap<String> tokenDictionary = constructTokenDictionary(collection);
		FuzzyIntSetCollection transformedCollection;
		if (keepOriginal)
			transformedCollection = transformCollectionExtended(collection, tokenDictionary);
		else
			transformedCollection = transformCollection(collection, tokenDictionary);

		if (cleanDuplicates) {
			int[] hashCodes = new int[transformedCollection.sets.length];
			TIntObjectMap<TIntSet> hashGroups = new TIntObjectHashMap<TIntSet>();

			for (int R = 0; R < transformedCollection.keys.length; R++) {
				int hash = collection.hashCodes.get(transformedCollection.keys[R]);
				hashCodes[R] = hash;
				if (!hashGroups.containsKey(hash))
					hashGroups.put(hash, new TIntHashSet());
				hashGroups.get(hash).add(R);
			}

			TIntObjectMap<TIntSet> cleanHashGroups = new TIntObjectHashMap<TIntSet>();
			TIntObjectIterator<TIntSet> it = hashGroups.iterator();
			while (it.hasNext()) {
				it.advance();
				if (it.value().size() > 1) // duplicate
					cleanHashGroups.put(it.key(), it.value());
			}

			transformedCollection.hashCodes = hashCodes;
			transformedCollection.hashGroups = cleanHashGroups;
		}

		return transformedCollection;
	}

	/** Reads input from a CSV file */
	private RawCollection fromCSV(JSONObject readConfig, JSONObject execConfig, boolean keepOriginal) {
		/* READ PARAMETERS */
		// input & output
		String file = String.valueOf(readConfig.get("input_file"));
		int maxLines = Integer.parseInt(String.valueOf(execConfig.get("max_lines")));
		int totalLines = Integer.parseInt(String.valueOf(execConfig.get("total_lines")));

		// file parsing
		int colSetId = Integer.parseInt(String.valueOf(readConfig.get("set_column"))) - 1;
		int colSetTokens = Integer.parseInt(String.valueOf(readConfig.get("tokens_column"))) - 1;

		String columnDelimiter = String.valueOf(readConfig.get("column_delimiter"));
		if (columnDelimiter.equals("null") || columnDelimiter.equals(""))
			columnDelimiter = " ";
		String elementDelimiter = String.valueOf(readConfig.get("element_delimiter"));
		if (elementDelimiter.equals("null") || elementDelimiter.equals(""))
			elementDelimiter = " ";
		String tokenDelimiter = null;
		if (readConfig.containsKey("token_delimiter"))
			tokenDelimiter = String.valueOf(readConfig.get("token_delimiter"));

		boolean header = Boolean.parseBoolean(String.valueOf(readConfig.get("header")));
		String tokenizer = "qgram";

		int qgram = 3;

		TIntList indices = new TIntArrayList();
		for (int i = 0; i < totalLines; i++)
			indices.add(i);
		indices.shuffle(new Random(1924));

		if (maxLines == -1)
			maxLines = totalLines;
		else
			maxLines = Math.min(maxLines, totalLines);
		TIntList subList = indices.subList(0, maxLines);
		TIntSet sample = new TIntHashSet(subList);
		int maxLine = subList.max();

		RawCollection collection = new RawCollection();

		double tokensPerElement = 0;
		double elementsPerSet = 0;
		BufferedReader br;
		int lines = -1;
		int errorLines = 0;

		Map<String, Integer> hashCodes = new HashMap<String, Integer>();

		try {
			br = new BufferedReader(new FileReader(file));

			String line;
			String[] columns;

			// if the file has header, ignore the first line
			if (header) {
				br.readLine();
			}

			while ((line = br.readLine()) != null) {
				lines++;
				try {
					if (!sample.contains(lines)) {
						continue;
					}
					columns = line.split(columnDelimiter);

					String record_id;
					if (colSetId < 0) {
						record_id = String.valueOf(lines);
					} else
						record_id = columns[colSetId];

					String[] words = columns[colSetTokens].toLowerCase().split(elementDelimiter);
					if (words.length < 1) {
						errorLines++;
						continue;
					}

					if (words.length > overLimit) {
						words = Arrays.copyOf(words, overLimit); // first 400 words
						over400++;
					}

					// Deduplication
					Arrays.sort(words);
					String tempString = Arrays.toString(words);
					int hash = tempString.hashCode();
					hashCodes.put(record_id, hash);

					ArrayList<ArrayList<String>> elements = new ArrayList<ArrayList<String>>();
					ArrayList<String> original = new ArrayList<String>();
					ArrayList<ArrayList<String>> elements2 = new ArrayList<ArrayList<String>>();

					for (String word : words) {
						ArrayList<String> tokens, tokens2;

						if (tokenDelimiter == null) {
							// Max $$ added.
							String word2 = word + StringUtils.repeat('$', qgram - 1);
							// Removed extra $ to fit qchunks
							int leftChars = word2.length() % qgram;
							word2 = word2.substring(0, word2.length() - leftChars);

							if (tokenizer.equals("qgram"))
								tokens = getTokens(word2, "qgram", qgram, null);
							else
								tokens = getTokens(word2, "word", qgram, null);
							
							if (tokens.size() == 0) {
								continue;
							}
							
							if (keepOriginal) {
								// Not saved word2, but word
								original.add(word2);
								tokens2 = getQChunks(word2, qgram, null);
								elements2.add(tokens2);
							}
							
						} else {
							tokens = new ArrayList<String>();
							for (String token : word.split(tokenDelimiter))
							tokens.add(token);
							
							if (tokens.size() == 0) {
								continue;
							}
						}
						
						elements.add(tokens);
						tokensPerElement += tokens.size();
					}
					if (elements.size() < 1) {
						errorLines++;
						continue;
					}

					if (keepOriginal)
						collection.putRecord(record_id, elements, original, elements2);
					else
						collection.putRecord(record_id, elements);
					elementsPerSet += elements.size();

				} catch (Exception e) {
					e.printStackTrace();
					errorLines++;
				}
				if (lines > maxLine) {
					break;
				}
			}

			br.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

		tokensPerElement /= elementsPerSet;
		elementsPerSet /= collection.size();

		System.out.println("Finished reading file. Lines read: " + lines + ". Lines skipped due to errors: "
				+ errorLines + ". Num of sets: " + collection.size() + ". Elements per set: " + elementsPerSet
				+ ". Tokens per Element: " + tokensPerElement);
//		System.out.println("Over 400 elements were: " + over400);

		collection.hashCodes = hashCodes;

		return collection;
	}

	/* Split element to tokens */
	private static ArrayList<String> getTokens(String line, String tokenizer, int qgram, String tokenDelimiter) {

		TObjectIntMap<String> tokens = new TObjectIntHashMap<String>();
		if (tokenizer.equals("qgram")) {
			String token = line;
			for (int i = 0; i <= token.length() - qgram; i++) {
				tokens.adjustOrPutValue(token.substring(i, i + qgram), 1, 1);
			}
		} else {
			if (tokenDelimiter != null) {
				for (String tok : Arrays.asList(line.split(tokenDelimiter))) {
					if (!tok.equals(""))
						tokens.adjustOrPutValue(tok, 1, 1);
				}
			} else {
				tokens.put(line, 1);
			}
		}

		ArrayList<String> tokens2 = new ArrayList<String>();
		for (String key : tokens.keySet()) {
			for (int val = 0; val < tokens.get(key); val++) {
				tokens2.add(key + "@" + val);
			}
		}

		return tokens2;
	}

	private static ArrayList<String> getQChunks(String line, int qgram, String tokenDelimiter) {

		TObjectIntMap<String> tokens = new TObjectIntHashMap<String>();
		String token = line;
		for (int i = 0; i <= token.length() - qgram; i += qgram) {
			tokens.adjustOrPutValue(token.substring(i, i + qgram), 1, 1);
		}

		ArrayList<String> tokens2 = new ArrayList<String>();
		for (String key : tokens.keySet()) {
			for (int val = 0; val < tokens.get(key); val++) {
				tokens2.add(key + "@" + val);
			}
		}

		return tokens2;
	}

	/* Construct Token Dictionary, String to Int */
	private TObjectIntMap<String> constructTokenDictionary(RawCollection collection) {

		// Sort tokens by frequency
		TokenFrequencyPair[] tfs = calculateTokenFrequency(collection.qgrams);

		// Assign integer IDs to tokens
		TObjectIntMap<String> tokenDict = new TObjectIntHashMap<String>();
		for (int i = 0; i < tfs.length; i++) {
			tokenDict.put(tfs[i].token, i);
		}

		return tokenDict;
	}

	/* Calculate frequency of string tokens */
	private TokenFrequencyPair[] calculateTokenFrequency(Map<String, ArrayList<ArrayList<String>>> collection) {
		// Compute token frequencies
		TObjectIntMap<String> tokenDict = new TObjectIntHashMap<String>();
		int frequency = 0;
		for (ArrayList<ArrayList<String>> record : collection.values()) {
			for (ArrayList<String> element : record) {
				for (String token : element) {
					frequency = tokenDict.get(token);
					frequency++;
					tokenDict.put(token, frequency);
				}
			}
		}

		// Sort tokens by frequency
		TokenFrequencyPair[] tfs = new TokenFrequencyPair[tokenDict.size()];
		TokenFrequencyPair tf;
		int counter = 0;
		for (String token : tokenDict.keySet()) {
			tf = new TokenFrequencyPair();
			tf.token = token;
			tf.frequency = tokenDict.get(token);
			tfs[counter] = tf;
			counter++;
		}

		Arrays.sort(tfs);

		return tfs;
	}

	/*
	 * Transform Collection from String tokens to Int tokens and sort by size. Made
	 * for Jaccard
	 */
	private FuzzyIntSetCollection transformCollection(RawCollection collection, TObjectIntMap<String> tokenDictionary) {

		boolean existingDictionary = tokenDictionary.size() > 0 ? true : false;
		int unknownTokenCounter = 0;

		int i = 0, j;
		ArrayList<ArrayList<String>> elements;
		FuzzyIntSet[] records = new FuzzyIntSet[collection.size()];

		for (String id : collection.qgrams.keySet()) {
			elements = collection.qgrams.get(id);
			records[i] = new FuzzyIntSet(id, elements.size());
			records[i].id = id;
			j = 0;
			for (List<String> element : elements) {
				records[i].addElement(j);
				for (String token : element) {
					if (!tokenDictionary.containsKey(token)) {
						if (existingDictionary) {
							unknownTokenCounter--;
							tokenDictionary.put(token, unknownTokenCounter);
						} else {
							tokenDictionary.put(token, tokenDictionary.size());
						}
					}
					records[i].addToken(j, tokenDictionary.get(token));
				}
				j++;
			}
			i++;
		}

		Arrays.sort(records);

		// Populate the collection
		FuzzyIntSetCollection transformedCollection = new FuzzyIntSetCollection(tokenDictionary.size(), records.length);

		for (int r = 0; r < transformedCollection.sets.length; r++) {
			transformedCollection.add(records[r], r);
		}

		return transformedCollection;
	}

	/*
	 * Transform Collection from String tokens to Int tokens and sort by size. Made
	 * for Edit
	 */
	private FuzzyIntSetCollection transformCollectionExtended(RawCollection collection,
			TObjectIntMap<String> tokenDictionary) {

		boolean existingDictionary = tokenDictionary.size() > 0 ? true : false;
		int unknownTokenCounter = 0;

		int i = 0;
		FuzzyIntSet[] records = new FuzzyIntSet[collection.size()];
		for (String id : collection.qgrams.keySet()) {
			ArrayList<ArrayList<String>> elements = collection.qgrams.get(id);
			ArrayList<ArrayList<String>> elements2 = collection.qchunks.get(id);
			ArrayList<String> originalStrings = collection.originalStrings.get(id);

			records[i] = new FuzzyIntSet(id, elements.size());
			records[i].id = id;
			for (int j = 0; j < elements.size(); j++) {
				records[i].addElement(j);
				for (String token : elements.get(j)) {
					if (!tokenDictionary.containsKey(token)) {
						if (existingDictionary) {
							unknownTokenCounter--;
							tokenDictionary.put(token, unknownTokenCounter);
						} else {
							tokenDictionary.put(token, tokenDictionary.size());
						}
					}
					records[i].addToken(j, tokenDictionary.get(token));
				}

				for (String token : elements2.get(j)) {
					records[i].addToken2(j, tokenDictionary.get(token));
				}
				records[i].addString(j, originalStrings.get(j));
			}
			i++;
		}

		Arrays.sort(records);

		// Populate the collection
		FuzzyIntSetCollection transformedCollection = new FuzzyIntSetCollection(tokenDictionary.size(), records.length);

		for (int r = 0; r < transformedCollection.sets.length; r++) {
			transformedCollection.add(records[r], r);
		}

		return transformedCollection;
	}

	private class TokenFrequencyPair implements Comparable<TokenFrequencyPair> {

		private String token;
		private int frequency;

		public int compareTo(TokenFrequencyPair tf) {
			int r = this.frequency == tf.frequency ? this.token.compareTo(tf.token) : this.frequency - tf.frequency;
			return r;
		}

		@Override
		public String toString() {
			return token + "->" + frequency;
		}
	}

	private class RawCollection {
		public Map<String, ArrayList<ArrayList<String>>> qchunks;
		public Map<String, ArrayList<ArrayList<String>>> qgrams;
		public Map<String, ArrayList<String>> originalStrings;
		public Map<String, Integer> hashCodes;

		public RawCollection() {
			qgrams = new HashMap<String, ArrayList<ArrayList<String>>>();
			originalStrings = new HashMap<String, ArrayList<String>>();
			qchunks = new HashMap<String, ArrayList<ArrayList<String>>>();
		}

		public void putRecord(String record_id, ArrayList<ArrayList<String>> elements) {
			qgrams.put(record_id, elements);
		}

		public void putRecord(String record_id, ArrayList<ArrayList<String>> elements, ArrayList<String> original,
				ArrayList<ArrayList<String>> elements2) {
			qgrams.put(record_id, elements);
			originalStrings.put(record_id, original);
			qchunks.put(record_id, elements2);
		}

		public int size() {
			return qgrams.size();
		}
	}
}