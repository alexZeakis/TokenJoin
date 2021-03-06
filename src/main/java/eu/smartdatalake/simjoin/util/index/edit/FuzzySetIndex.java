package eu.smartdatalake.simjoin.util.index.edit;

import java.util.Arrays;

import eu.smartdatalake.simjoin.util.collection.FuzzyIntSetCollection;
import gnu.trove.list.TIntList;
import gnu.trove.list.array.TIntArrayList;
import gnu.trove.map.TIntObjectMap;
import gnu.trove.map.hash.TIntObjectHashMap;

/**
 * Index of records made for Edit Similarity.
 *
 */
public class FuzzySetIndex {

	/**
	 * Inverted list for each token.
	 */
	public int[][] lengths;

	/**
	 * Token Information per Record. Check {@link IndexTokenScore IndexTokenScore}
	 * for more information.
	 */
	public TIntObjectMap<IndexTokenScore>[] idx;

	/**
	 * Method to construct index.
	 * 
	 * @param collection: Collection of records
	 */
	@SuppressWarnings("unchecked")
	public void buildIndex(FuzzyIntSetCollection collection) {
		TIntList[] lengthsList = new TIntList[collection.numTokens];

		for (int i = 0; i < collection.numTokens; i++) {
			lengthsList[i] = new TIntArrayList();
		}

		double q = 3.0;

		idx = new TIntObjectMap[collection.sets.length];
		// populate idx
		for (int i = 0; i < collection.sets.length; i++) {
			TIntObjectHashMap<TIntList> qtokElemsList = new TIntObjectHashMap<TIntList>();
			TIntObjectHashMap<TIntList> tokElemsList = new TIntObjectHashMap<TIntList>();

			for (int j = 0; j < collection.qsets[i].length; j++) {
				for (int token : collection.qsets[i][j]) {
					if (!qtokElemsList.containsKey(token)) {
						qtokElemsList.put(token, new TIntArrayList());
					}
					qtokElemsList.get(token).add(j);
				}

				for (int token : collection.sets[i][j]) {
					if (!tokElemsList.containsKey(token)) {
						tokElemsList.put(token, new TIntArrayList());
					}
					tokElemsList.get(token).add(j);
				}
			}

			int[] tokenIDs = tokElemsList.keys();
			Arrays.parallelSort(tokenIDs);

			// fill valuesReverse
			idx[i] = new TIntObjectHashMap<IndexTokenScore>();
			double UB = collection.sets[i].length;

			double UB2 = collection.sets[i].length * (q - 1) / q; // hidden score

			for (int j = 0; j < collection.sets[i].length; j++) {
				UB2 += (collection.originalStrings[i][j].length() - q + 1) / collection.originalStrings[i][j].length(); // (|r|
																														// -
																														// q)
																														// qgrams
																														// that
																														// all
																														// have
			}

			for (int token : tokenIDs) {
				IndexTokenScore tok = new IndexTokenScore(token, qtokElemsList.get(token), tokElemsList.get(token),
						collection.originalStrings[i]);
				UB -= tok.value;
				tok.rest = UB;

				UB2 -= tok.value2;
				tok.rest2 = UB2;

				idx[i].put(token, tok);

				lengthsList[token].add(i);
			}
		}

		lengths = new int[collection.numTokens][];
		for (int i = 0; i < collection.numTokens; i++) {
			lengths[i] = lengthsList[i].toArray();
		}

	}

}