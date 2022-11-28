package gr.athenarc.imsi.simjoin.util.index.edit;

import gnu.trove.list.TIntList;

/**
 * Class for information of a token for a specific record. Implemented for Edit
 * Similarity.
 *
 */
public class IndexTokenScore2 implements Comparable<IndexTokenScore2> {
	/**
	 * ID of token.
	 */
	public int id;
	/**
	 * Total Utility of token when as qchunk.
	 */
	public double value;

	/**
	 * Elements of record that contain this token.
	 */
	public int[] elements;

	public IndexTokenScore2(int value, TIntList qelements, TIntList elements, String[] R) {
		this.id = value;
		this.elements = elements.toArray();

		if (qelements == null) {
			this.value = 0;
		} else {
			this.value = 0;
			for (int i = 0; i < qelements.size(); i++) {
				this.value += 1.0 / R[qelements.get(i)].length();
			}
		}

	}

	@Override
	public int compareTo(IndexTokenScore2 o) {
		return Double.compare(this.id, o.id);
	}

	@Override
	public String toString() {
		return String.format("[%s, %.5f]", id, value);
	}
}