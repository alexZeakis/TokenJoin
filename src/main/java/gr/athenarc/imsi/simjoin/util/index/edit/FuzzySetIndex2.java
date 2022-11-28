package gr.athenarc.imsi.simjoin.util.index.edit;

import java.util.Arrays;

import gnu.trove.list.TIntList;
import gnu.trove.list.array.TIntArrayList;
import gnu.trove.map.TIntObjectMap;
import gnu.trove.map.hash.TIntObjectHashMap;
import gr.athenarc.imsi.simjoin.util.collection.FuzzyIntSetCollection;

/**
 * Index of records made for Edit Similarity.
 *
 */
public class FuzzySetIndex2 {

	/**
	 * Token Information per Record. Check {@link IndexTokenScore IndexTokenScore}
	 * for more information.
	 */
	public TIntObjectMap<IndexTokenScore2>[] idx;

	/**
	 * Method to construct index.
	 * 
	 * @param collection: Collection of records
	 */
	
	public int[][] recordIndex;
	public int[][] elementIndex;
	
	@SuppressWarnings("unchecked")
	public FuzzySetIndex2(FuzzyIntSetCollection collection) {
		idx = new TIntObjectMap[collection.sets.length];
		
		TIntObjectHashMap<TIntList> qtokElemsList = new TIntObjectHashMap<TIntList>();
		TIntObjectHashMap<TIntList> tokElemsList = new TIntObjectHashMap<TIntList>();
		// populate idx
		for (int i = 0; i < collection.sets.length; i++) {
			qtokElemsList.clear();
			tokElemsList.clear();

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
			idx[i] = new TIntObjectHashMap<IndexTokenScore2>();
			for (int token : tokenIDs) {
				IndexTokenScore2 tok = new IndexTokenScore2(token, qtokElemsList.get(token), tokElemsList.get(token),
						collection.originalStrings[i]);
				idx[i].put(token, tok);
			}
		}

		TIntList[] recordIndexList = new TIntList[collection.numTokens];
		TIntList[] elementIndexList = new TIntList[collection.numTokens];
		for (int tok = 0; tok < collection.numTokens; tok++) {
			recordIndexList[tok] = new TIntArrayList();
			elementIndexList[tok] = new TIntArrayList();
		}

		for (int i = 0; i < collection.sets.length; i++) {
			for (int j = 0; j < collection.sets[i].length; j++) {
				for (int token : collection.sets[i][j]) {
					recordIndexList[token].add(i);
					elementIndexList[token].add(j);
				}
			}
		}
		
		recordIndex = new int[collection.numTokens][];
		elementIndex = new int[collection.numTokens][];
		for (int tok = 0; tok < collection.numTokens; tok++) {
			recordIndex[tok] = recordIndexList[tok].toArray();
			elementIndex[tok] = elementIndexList[tok].toArray();
		}

	}

}