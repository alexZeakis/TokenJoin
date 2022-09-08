package eu.smartdatalake.simjoin.util.collection;

import java.util.Arrays;

import eu.smartdatalake.simjoin.util.collection.FuzzyIntSet.PublicFuzzySet;
import gnu.trove.iterator.TIntObjectIterator;
import gnu.trove.list.TIntList;
import gnu.trove.list.array.TIntArrayList;
import gnu.trove.map.TIntObjectMap;
import gnu.trove.map.hash.TIntObjectHashMap;
import gnu.trove.set.TIntSet;

import java.io.Serializable;

/**
 * A class that contains all necessary information to perform a Similarity Join.
 *
 */
public class FuzzyIntSetCollection implements Serializable {
	private static final long serialVersionUID = 1L;

	/**
	 * 3D-Int Array containing the tokens. First dimension is a record, second an
	 * element and last a token.
	 */
	public int[][][] sets;

	/**
	 * String array containing the original keys of records.
	 */
	public String[] keys;

	/**
	 * Number of unique tokens in collection.
	 */
	public int numTokens;
	/**
	 * Array containing duplicate elements. Used in Verification.
	 */
	private Clustering[][] clusterings;
	/**
	 * Array containing original string. Used in edit similarity.
	 */
	public String[][] originalStrings;
	/**
	 * 3D Array containing qchunks. Only for Edit.
	 */
	public int[][][] qsets;

	/**
	 * Array of hashcodes of each record. Used for record deduplication.
	 */
	public int[] hashCodes;

	/**
	 * Map of clusters of duplicate records. Used for record deduplication.
	 */
	public TIntObjectMap<TIntSet> hashGroups;

	FuzzyIntSetCollection(int numTokens, int len) {
		this.numTokens = numTokens;
		this.sets = new int[len][][];
		this.keys = new String[len];

		this.qsets = new int[len][][];
		this.originalStrings = new String[len][];

		this.clusterings = new Clustering[len][];
	}

	void add(FuzzyIntSet set, int i) {
		PublicFuzzySet pfe = set.getTokens();

		this.originalStrings[i] = pfe.originalString;
		this.qsets[i] = pfe.tokens2;
		this.sets[i] = pfe.tokens;
		this.keys[i] = set.id;
	}

	/**
	 * Clear previously calculated clusterings. This is necessary when comparing
	 * multiple algorithms on the same collection, since cached clusterings will
	 * advantage following algorithms.
	 */
	public void clearClusterings() {
		this.clusterings = new Clustering[this.clusterings.length][];
	}

	/**
	 * @param index: index of record
	 * @return {@link Clustering Clustering} of record. Calculated on demand and
	 *         cached afterwards.
	 */
	public Clustering[] getClustering(int index) {
		if (clusterings[index] == null) {
			TIntObjectHashMap<TIntList> tempClustering = new TIntObjectHashMap<TIntList>();
			int[][] r = sets[index];
			for (int i = 0; i < r.length; i++) {
				int h = Arrays.hashCode(r[i]);
				if (!tempClustering.containsKey(h))
					tempClustering.put(h, new TIntArrayList());
				tempClustering.get(h).add(i);
			}

			clusterings[index] = new Clustering[tempClustering.size()];
			TIntObjectIterator<TIntList> it = tempClustering.iterator();
			int i = 0;
			while (it.hasNext()) {
				it.advance();
				clusterings[index][i++] = new Clustering(it.key(), it.value());
			}
			Arrays.sort(clusterings[index]);
		}
		return clusterings[index];
	}

}