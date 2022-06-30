package eu.smartdatalake.simjoin.util.collection;

import java.util.Arrays;

import gnu.trove.list.TIntList;
import gnu.trove.list.array.TIntArrayList;

class FuzzyIntSet implements Comparable<FuzzyIntSet> {

	String id;
	FuzzyElement[] elements;

	FuzzyIntSet(String id, int length) {
		this.id = id;
		this.elements = new FuzzyElement[length];
	}

	public int compareTo(FuzzyIntSet s) {
		return Integer.compare(this.elements.length, s.elements.length);
	}

	void addToken(int element, int token) {
		elements[element].addToken(token);
	}

	void addToken2(int element, int token) {
		elements[element].addToken2(token);
	}

	void addString(int element, String string) {
		elements[element].setString(string);
	}

	void addElement(int i) {
		elements[i] = new FuzzyElement();
	}

	PublicFuzzySet getTokens() {
		PublicFuzzySet pfe = new PublicFuzzySet(elements.length);
		Arrays.sort(elements);

		for (int i = 0; i < elements.length; i++) {
			pfe.add(i, elements[i]);
		}
		return pfe;
	}

	class FuzzyElement implements Comparable<FuzzyElement> {
		TIntList tokens;
		TIntList tokens2;
		String originalString;

		FuzzyElement() {
			tokens = new TIntArrayList();
			tokens2 = new TIntArrayList();
		}

		void addToken(int token) {
			tokens.add(token);
		}

		void addToken2(int token) {
			tokens2.add(token);
		}

		void setString(String string) {
			originalString = string;
		}

		@Override
		public int compareTo(FuzzyElement o) {
			return Integer.compare(this.tokens.size(), o.tokens.size());
		}
	}

	class PublicFuzzySet {
		int[][] tokens;
		int[][] tokens2;
		String[] originalString;

		PublicFuzzySet(int length) {
			tokens = new int[length][];
			tokens2 = new int[length][];
			originalString = new String[length];
		}

		void add(int i, FuzzyElement fe) {
			fe.tokens.sort();
			tokens[i] = fe.tokens.toArray();
			fe.tokens2.sort();
			tokens2[i] = fe.tokens2.toArray();
			originalString[i] = fe.originalString;
		}
	}
}