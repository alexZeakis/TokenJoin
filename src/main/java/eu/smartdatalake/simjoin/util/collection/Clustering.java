package eu.smartdatalake.simjoin.util.collection;

import gnu.trove.list.TIntList;

/**
 * Helping class for grouping duplicate elements.
 *
 */
public class Clustering implements Comparable<Clustering> {
	public int h;
	public TIntList elements;

	public Clustering(int h, TIntList elements) {
		this.h = h;
		this.elements = elements;
	}

	@Override
	public int compareTo(Clustering o) {
		return Integer.compare(h, o.h); // ascending
	}

	@Override
	public String toString() {
		return String.format("%s -> %s", h, elements);
	}
}
