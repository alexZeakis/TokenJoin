package eu.smartdatalake.simjoin.util.index.jaccard;

import gnu.trove.list.TIntList;

/**
 * Class for information of a token for a specific record. Implemented for
 * Jaccard Similarity.
 *
 */
public class IndexTokenScore implements Comparable<IndexTokenScore> {
	/**
	 * ID of token.
	 */
	public int id;
	/**
	 * Total Utility of token.
	 */
	public double value;

	/**
	 * Utilities of token.
	 */
	public double[] utilities;

	/**
	 * Remaining utility of record after this token.
	 */
	public double rest;

	/**
	 * Elements of record that contain this token.
	 */

	public IndexTokenScore(int value, TIntList elements, int[][] R) {
		this.id = value;
		this.utilities = new double[elements.size()];

		double start = 0;
		for (int i = 0; i < elements.size(); i++) {
			this.utilities[i] = start + 1.0 / R[elements.get(i)].length;
			start = this.utilities[i];
		}
		this.value = this.utilities[this.utilities.length - 1];
	}

	@Override
	public int compareTo(IndexTokenScore o) {
		return Double.compare(this.id, o.id);
	}

	@Override
	public String toString() {
		return String.format("(%s, %.5f, %.5f)", id, value, rest);
	}
}