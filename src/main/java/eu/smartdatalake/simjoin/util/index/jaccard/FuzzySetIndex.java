package eu.smartdatalake.simjoin.util.index.jaccard;

import java.util.Arrays;

import eu.smartdatalake.simjoin.util.collection.FuzzyIntSetCollection;
import gnu.trove.list.TIntList;
import gnu.trove.list.array.TIntArrayList;
import gnu.trove.map.TIntObjectMap;
import gnu.trove.map.hash.TIntObjectHashMap;

/**
 * Index of records made for Jaccard Similarity.
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

		idx = new TIntObjectMap[collection.sets.length];
		
		TIntObjectHashMap<TIntList> tokElemsList = new TIntObjectHashMap<TIntList>();
		// populate idx
		for (int i = 0; i < collection.sets.length; i++) {
//			TIntObjectHashMap<TIntList> tokElemsList = new TIntObjectHashMap<TIntList>();
			tokElemsList.clear();

			for (int j = 0; j < collection.sets[i].length; j++) {
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
			for (int token : tokenIDs) {
				IndexTokenScore tok = new IndexTokenScore(token, tokElemsList.get(token), collection.sets[i]);

				UB -= tok.value;
				tok.rest = UB;
				idx[i].put(token, tok);
				lengthsList[token].add(i);
			}
		}

		lengths = new int[collection.numTokens][];
		for (int i = 0; i < collection.numTokens; i++) {
			lengths[i] = lengthsList[i].toArray();
			lengthsList[i].clear();
		}
		lengthsList = null;
	}

}