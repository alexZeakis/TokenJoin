package eu.smartdatalake.simjoin.util.index.edit;

import gnu.trove.list.TIntList;

/**
 * Class for information of a token for a specific record. Implemented for Edit
 * Similarity.
 *
 */
public class IndexTokenScore implements Comparable<IndexTokenScore> {
	/**
	 * ID of token.
	 */
	public int id;
	/**
	 * Total Utility of token when as qchunk.
	 */
	public double value;

	/**
	 * Utilities of token when as qchunk.
	 */
	public double[] utilities;

	/**
	 * Remaining utility of record after this token when a qchunk.
	 */
	public double rest;

	/**
	 * Total Utility of token when as qgram.
	 */
	public double value2;

	/**
	 * Utilities of token when as qgram.
	 */
	public double[] utilities2;

	/**
	 * Remaining utility of record after this token when a gram.
	 */
	public double rest2;

	/**
	 * Elements of record that contain this token.
	 */
	public int[] elements;

	public IndexTokenScore(int value, TIntList qelements, TIntList elements, String[] R) {
		this.id = value;
		this.elements = elements.toArray();

		if (qelements == null) {
			this.utilities = new double[0];
			this.value = 0;
		} else {
			this.utilities = new double[qelements.size()];

			double start = 0;
			for (int i = 0; i < qelements.size(); i++) {
				this.utilities[i] = start + 1.0 / R[qelements.get(i)].length();
				start = this.utilities[i];
			}
			this.value = this.utilities[this.utilities.length - 1];
		}

		this.utilities2 = new double[elements.size()];

		double start = 0;
		for (int i = 0; i < elements.size(); i++) {
			this.utilities2[i] = start + 1.0 / R[elements.get(i)].length();
			start = this.utilities2[i];
		}
		this.value2 = this.utilities2[this.utilities2.length - 1];

	}

	@Override
	public int compareTo(IndexTokenScore o) {
		return Double.compare(this.id, o.id);
	}

	@Override
	public String toString() {
		return String.format("[%s, (%.5f, %.5f), (%.5f, %.5f)]", id, value, rest, value2, rest2);
	}
}