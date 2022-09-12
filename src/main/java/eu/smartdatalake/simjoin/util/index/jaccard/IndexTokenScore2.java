package eu.smartdatalake.simjoin.util.index.jaccard;

import gnu.trove.list.TIntList;

/**
 * Class for information of a token for a specific record. Implemented for
 * Jaccard Similarity.
 *
 */
public class IndexTokenScore2 implements Comparable<IndexTokenScore2> {
	/**
	 * ID of token.
	 */
	public int id;
	/**
	 * Total Utility of token.
	 */
	public double value;

	/**
	 * Elements of record that contain this token.
	 */
	public int[] elements;

	public IndexTokenScore2(int value, TIntList elements, int[][] R) {
		this.id = value;
		this.elements = elements.toArray();

		this.value = 0;
		for (int i = 0; i < elements.size(); i++) {
			this.value += 1.0 / R[elements.get(i)].length;
		}
	}

	@Override
	public int compareTo(IndexTokenScore2 o) {
		return Double.compare(this.id, o.id);
	}

	@Override
	public String toString() {
		return String.format("(%s, %.5f)", id, value);
	}
}