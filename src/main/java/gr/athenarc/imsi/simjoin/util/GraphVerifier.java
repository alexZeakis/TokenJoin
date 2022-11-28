package gr.athenarc.imsi.simjoin.util;

import java.util.HashSet;
import java.util.Iterator;
import java.util.PriorityQueue;
import java.util.Set;

import org.json.simple.JSONObject;

import gnu.trove.iterator.TIntIterator;
import gnu.trove.list.TIntList;
import gnu.trove.list.array.TIntArrayList;
import gnu.trove.set.TIntSet;
import gnu.trove.set.hash.TIntHashSet;
import gr.athenarc.imsi.simjoin.util.collection.Clustering;

/**
 * Class for Maximum Weighted Bipartite Matching computation.
 *
 */
public class GraphVerifier {
	static long UBSaved1, LBSaved1, UBSaved2, LBSaved2;
	static long dedupTime = 0, initHitsTime = 0, initPiTime = 0, matchingTime = 0;

	/**
	 * Method for removing exact matched in bipartite.
	 * 
	 * @param r:         Clustering of left partition.
	 * @param s:         Clustering of right partition.
	 * @param rVertices: Nodes of left partition.
	 * @param sVertices: Nodes of right partition.
	 * @return Number of exact matches to add to final matching.
	 */
	double deduplicateGraph(Clustering[] r, Clustering[] s, TIntSet rVertices, TIntSet sVertices) {

		double add = 0;
		int pr = 0, ps = 0;

		while (pr < r.length && ps < s.length) {

			if (r[pr].h == s[ps].h) { // identical elements
				if (r[pr].elements.size() > s[ps].elements.size()) { // R has more, some will be spare
					for (int i = 0; i < r[pr].elements.size() - s[ps].elements.size(); i++)
						rVertices.add(r[pr].elements.get(i));
					add += s[ps].elements.size();
				} else { // S has more, some will be spare
					for (int i = 0; i < s[ps].elements.size() - r[pr].elements.size(); i++)
						sVertices.add(s[ps].elements.get(i));
					add += r[pr].elements.size();
				}
				pr++;
				ps++;
			} else if (r[pr].h < s[ps].h) { // R elements are unique
				for (int i = 0; i < r[pr].elements.size(); i++)
					rVertices.add(r[pr].elements.get(i));
				pr++;
			} else { // S elements are unique
				for (int i = 0; i < s[ps].elements.size(); i++)
					sVertices.add(s[ps].elements.get(i));
				ps++;
			}
		}

		while (pr < r.length) { // R is not done
			for (int i = 0; i < r[pr].elements.size(); i++)
				rVertices.add(r[pr].elements.get(i));
			pr++;
		}

		while (ps < s.length) { // S is not done
			for (int i = 0; i < s[ps].elements.size(); i++)
				sVertices.add(s[ps].elements.get(i));
			ps++;
		}

		return add;
	}

	/* Hungarian Algorithm + Upper Bounds */
	double findMatchingUB(double[][] pi, double add, double[][] hits2, double[] nnEdges, double persThreshold) {
		Set<Edge> M = new HashSet<Edge>();
		int rLen = pi.length;
		int sLen = pi.length; // square matrix

		Graph g = new Graph(pi, rLen, sLen);

		int ROffset = 0;
		int SOffset = rLen + ROffset;
		double sumMatching = 0;

		while (M.size() != rLen) {
			// find augmenting path
			int pred[] = new int[g.V];
			boolean visited[] = new boolean[g.V];

			int finalNode = g.BFS(g.src, g.dest, pred, visited);

			if (finalNode == g.dest) { // successful augmenting path
				int crawl = pred[finalNode];
				Set<Edge> P = new HashSet<Edge>();
				while (pred[crawl] != g.src) {
					P.add(new Edge(crawl, pred[crawl]));
					crawl = pred[crawl];
				}

				// search succesfful -> augment Matching
				Set<Edge> M2 = new HashSet<Edge>();
				Iterator<Edge> it2 = M.iterator();
				while (it2.hasNext()) {
					Edge e = it2.next();
					if (!P.contains(e))
						M2.add(e);
				}
				it2 = P.iterator();
				while (it2.hasNext()) {
					Edge e = it2.next();
					if (!M.contains(e))
						M2.add(e);
				}

				for (Edge e : M)
					g.revertEdge(e); // remove reversal of edges
				M = M2;
				sumMatching = add;
				for (Edge e : M) {
					g.updateEdge(e);
					int r = e.src - ROffset;
					int s = e.dest - SOffset;
					sumMatching += hits2[r][s];
					nnEdges[r] = hits2[r][s];
				}

				double UB = sumMatching;
				TIntIterator unmatched = g.adj[g.src].iterator();
				while (unmatched.hasNext()) {
					int r = unmatched.next();
					UB += nnEdges[r];
				}

				if (persThreshold - UB > 0.0000001) {
					UBSaved2++;
					return UB;
				}

			} else { // landed in left Partite, we have a collision, variables need adjustment

				// find delta
				double delta = 1.1;

				// search delta (min pi) in Marked R -> Unmarked S
				for (int r = 0; r < rLen; r++) {
					if (visited[r]) { // if R is marked
						for (int s = 0; s < sLen; s++) {
							if (!visited[s + SOffset]) // if S is unmarked
								delta = Math.min(delta, pi[r][s]);
						}
					}
				}

				// reduce Marked R -> Unmarked S, to enable more edges
				for (int r = 0; r < rLen; r++) {
					if (visited[r]) {
						for (int s = 0; s < sLen; s++) {
							if (!visited[s + SOffset]) { // if S is unmarked
								pi[r][s] = pi[r][s] - delta;
								if (pi[r][s] == 0.0) {
									g.addEdge(r, s);
								}
							}
						}
					}
				}

				// increase unmarked R -> marked S, to discourage colliding edges
				for (int r = 0; r < rLen; r++) {
					if (!visited[r]) { // if R is unmarked
						for (int s = 0; s < sLen; s++) {
							if (visited[s + SOffset]) {// if S is marked
								pi[r][s] = pi[r][s] + delta;
								if (pi[r][s] != 0.0) {
									g.removeEdge(r, s);
								}
							}
						}
					}
				}
			}
		}

		return sumMatching;
	}

	/* Hungarian Algorithm + Upper & Lower Bounds */
	private double findMatchingULB(double[][] pi, double add, double[][] hits2, double[] nnEdges, double persThreshold,
			PriorityQueue<Element>[] pq) {
		Set<Edge> M = new HashSet<Edge>();
		int rLen = pi.length;
		int sLen = pi.length; // square matrix

		Graph g = new Graph(pi, rLen, sLen);

		int ROffset = 0;
		int SOffset = rLen + ROffset;
		double sumMatching = 0;

		int[] LBEdges = new int[rLen]; // store assignments for LB
		for (int r = 0; r < rLen; r++)
			LBEdges[r] = -1;

		while (M.size() != rLen) {
			// find augmenting path
			int pred[] = new int[g.V];
			boolean visited[] = new boolean[g.V];

			int finalNode = g.BFS(g.src, g.dest, pred, visited);

			if (finalNode == g.dest) { // successful augmenting path
				int crawl = pred[finalNode];
				Set<Edge> P = new HashSet<Edge>();
				while (pred[crawl] != g.src) {
					P.add(new Edge(crawl, pred[crawl]));
					crawl = pred[crawl];
				}

				// search succesfful -> augment Matching
				Set<Edge> M2 = new HashSet<Edge>();
				Iterator<Edge> it2 = M.iterator();
				while (it2.hasNext()) {
					Edge e = it2.next();
					if (!P.contains(e))
						M2.add(e);
				}
				it2 = P.iterator();
				while (it2.hasNext()) {
					Edge e = it2.next();
					if (!M.contains(e))
						M2.add(e);
				}

				for (Edge e : M)
					g.revertEdge(e); // remove reversal of edges
				M = M2;
				sumMatching = add;
				for (Edge e : M) {
					g.updateEdge(e);
					int r = e.src - ROffset;
					int s = e.dest - SOffset;
					sumMatching += hits2[r][s];
					nnEdges[r] = hits2[r][s];
				}

				double UB = sumMatching;
				TIntIterator unmatched = g.adj[g.src].iterator();
				while (unmatched.hasNext()) {
					int r = unmatched.next();
					UB += nnEdges[r];
				}

				if (persThreshold - UB > 0.0000001) {
					UBSaved2++;
					return UB;
				}

				double LB = sumMatching;
				unmatched = g.adj[g.src].iterator();
				while (unmatched.hasNext()) {
					int r = unmatched.next();

					if (pq[r] == null) // dummy left node
						continue;

					if (g.adj[SOffset + LBEdges[r]].size() == 0) // this LB is still free
						continue;

					while (!pq[r].isEmpty() && g.adj[SOffset + (pq[r].peek()).s].size() > 0) { // iterate, until first
																								// best unmatched
						pq[r].poll();
					}
					if (!pq[r].isEmpty()) {
						LB += pq[r].peek().score;
						LBEdges[r] = pq[r].peek().s;
					}
				}

				if (LB - persThreshold > 0.0000001) {
					LBSaved2++;
					return LB;
				}

			} else { // landed in left Partite, we have a collision, variables need adjustment

				// find delta
				double delta = 1.1;

				// search delta (min pi) in Marked R -> Unmarked S
				for (int r = 0; r < rLen; r++) {
					if (visited[r]) { // if R is marked
						for (int s = 0; s < sLen; s++) {
							if (!visited[s + SOffset]) // if S is unmarked
								delta = Math.min(delta, pi[r][s]);
						}
					}
				}

				// reduce Marked R -> Unmarked S, to enable more edges
				for (int r = 0; r < rLen; r++) {
					if (visited[r]) {
						for (int s = 0; s < sLen; s++) {
							if (!visited[s + SOffset]) { // if S is unmarked
								pi[r][s] = pi[r][s] - delta;
								if (pi[r][s] == 0.0) {
									g.addEdge(r, s);
								}
							}
						}
					}
				}

				// increase unmarked R -> marked S, to discourage colliding edges
				for (int r = 0; r < rLen; r++) {
					if (!visited[r]) { // if R is unmarked
						for (int s = 0; s < sLen; s++) {
							if (visited[s + SOffset]) {// if S is marked
								pi[r][s] = pi[r][s] + delta;
								if (pi[r][s] != 0.0) {
									g.removeEdge(r, s);
								}
							}
						}
					}
				}
			}
		}

		return sumMatching;
	}

	/* Hungarian Algorithm */
	private double findMatching(double[][] pi, double add, double[][] hits2) {
		Set<Edge> M = new HashSet<Edge>();
		int rLen = pi.length;
		int sLen = pi.length; // square matrix

		Graph g = new Graph(pi, rLen, sLen);

		int ROffset = 0;
		int SOffset = rLen + ROffset;
		double sumMatching = 0;

		while (M.size() != rLen) {
			// find augmenting path
			int pred[] = new int[g.V];
			boolean visited[] = new boolean[g.V];

			int finalNode = g.BFS(g.src, g.dest, pred, visited);

			if (finalNode == g.dest) { // successful augmenting path
				int crawl = pred[finalNode];
				Set<Edge> P = new HashSet<Edge>();
				while (pred[crawl] != g.src) {
					P.add(new Edge(crawl, pred[crawl]));
					crawl = pred[crawl];
				}

				// search succesfful -> augment Matching
				Set<Edge> M2 = new HashSet<Edge>();
				Iterator<Edge> it2 = M.iterator();
				while (it2.hasNext()) {
					Edge e = it2.next();
					if (!P.contains(e))
						M2.add(e);
				}
				it2 = P.iterator();
				while (it2.hasNext()) {
					Edge e = it2.next();
					if (!M.contains(e))
						M2.add(e);
				}

				for (Edge e : M)
					g.revertEdge(e); // remove reversal of edges
				M = M2;
				sumMatching = add;
				for (Edge e : M) {
					g.updateEdge(e);
					int r = e.src - ROffset;
					int s = e.dest - SOffset;
					sumMatching += hits2[r][s];
				}

			} else { // landed in left Partite, we have a collision, variables need adjustment

				// find delta
				double delta = 1.1;

				// search delta (min pi) in Marked R -> Unmarked S
				for (int r = 0; r < rLen; r++) {
					if (visited[r]) { // if R is marked
						for (int s = 0; s < sLen; s++) {
							if (!visited[s + SOffset]) // if S is unmarked
								delta = Math.min(delta, pi[r][s]);
						}
					}
				}

				// reduce Marked R -> Unmarked S, to enable more edges
				for (int r = 0; r < rLen; r++) {
					if (visited[r]) {
						for (int s = 0; s < sLen; s++) {
							if (!visited[s + SOffset]) { // if S is unmarked
								pi[r][s] = pi[r][s] - delta;
								if (pi[r][s] == 0.0) {
									g.addEdge(r, s);
								}
							}
						}
					}
				}

				// increase unmarked R -> marked S, to discourage colliding edges
				for (int r = 0; r < rLen; r++) {
					if (!visited[r]) { // if R is unmarked
						for (int s = 0; s < sLen; s++) {
							if (visited[s + SOffset]) {// if S is marked
								pi[r][s] = pi[r][s] + delta;
								if (pi[r][s] != 0.0) {
									g.removeEdge(r, s);
								}
							}
						}
					}
				}
			}
		}

		return sumMatching;
	}

	/**
	 * Method to calculate Maximum Weighted Bipartite Matching (Jaccard).
	 * 
	 * @param R:             Tokens of left Partition.
	 * @param S:             Tokens of right Partition.
	 * @param hits:          Array of element scores. Depending on the algorithm,
	 *                       not all scores could have been calculated before.
	 * @param clusteringR:   Clustering of left Partition for element deduplication.
	 * @param clusteringS:   Clustering of right Partition for element
	 *                       deduplication.
	 * @param persThreshold: θ for this specific pair: θ = δ / (1+δ) * (|R|+|S|)
	 * @param alg:           Algorithm to perform: 0 is classic Hungarian Algorithm,
	 *                       1 is for Hungarian + Upper Bounds and 2 is for
	 *                       Hungarian + Upper {@literal &} Lower Bounds.
	 * @return Record Similarity based on matching m, i.e. m / (|R|+|S|-m).
	 */
	@SuppressWarnings("unchecked")
	public double verifyGraph(int[][] R, int[][] S, double[][] hits, Clustering[] clusteringR, Clustering[] clusteringS,
			double persThreshold, int alg) {
		long localStartTime = System.nanoTime();
		TIntSet rVertices = new TIntHashSet();
		TIntSet sVertices = new TIntHashSet();

		double add = deduplicateGraph(clusteringR, clusteringS, rVertices, sVertices);

		int originalRLen = R.length;
		int originalSLen = S.length;
		dedupTime += System.nanoTime() - localStartTime;

		if (alg == 2 && add - persThreshold > 0.0000001) {
			LBSaved1++;
			return add / (originalRLen + originalSLen - add);
		}

		int rLen = rVertices.size();
		int sLen = sVertices.size();

		if (rLen == 0) { // no non-identical left nodes are left for matching
			return add / (originalRLen + originalSLen - add);
		}

		localStartTime = System.nanoTime();
		double[] colMin = new double[sLen];
		boolean square = false;
		if (rLen == sLen) { // square matrix
			for (int s = 0; s < sLen; s++)
				colMin[s] = 1.0;
			square = true;
		} else {
			rLen = sLen; // square matrix
			square = false;
		}

		double[][] hits2 = new double[rLen][];
		for (int r = 0; r < hits2.length; r++)
			hits2[r] = new double[sLen];

		TIntIterator rit, sit;
		double[] nnEdges = new double[rLen];

		PriorityQueue<Element>[] pq = null;
		if (alg == 2)
			pq = new PriorityQueue[rLen];

		double UB = add + rVertices.size();
		rit = rVertices.iterator();
		int ri = 0;
		while (rit.hasNext()) {
			int r = rit.next();

			if (alg == 2)
				pq[ri] = new PriorityQueue<Element>();

			sit = sVertices.iterator();
			int si = 0;
			while (sit.hasNext()) {
				int s = sit.next();
				if (hits != null && hits[r] != null && hits[r][s] > 0.0)
					hits2[ri][si] = hits[r][s];
				else
					hits2[ri][si] = Verification.verifyWithScore(R[r], S[s]);

				nnEdges[ri] = Math.max(nnEdges[ri], hits2[ri][si]);
				if (alg == 2)
					pq[ri].add(new Element(si, hits2[ri][si]));

				si++;
			}
//			if (alg > 0) { // UB & LB
				UB -= 1 - nnEdges[ri];
				if (persThreshold - UB > 0.0000001) {
					UBSaved1++;
					initHitsTime += System.nanoTime() - localStartTime;
					return UB / (originalRLen + originalSLen - UB);
				}
//			}
			ri++;
		}
		initHitsTime += System.nanoTime() - localStartTime;

		localStartTime = System.nanoTime();
		double[][] pi = new double[rLen][];

		// initialize: inverse and subtract row minima
		for (int r = 0; r < rLen; r++) {
			pi[r] = new double[sLen];
			for (int s = 0; s < sLen; s++) {
				pi[r][s] = nnEdges[r] - hits2[r][s];
				if (square)
					colMin[s] = Math.min(colMin[s], pi[r][s]);
			}

		}

		// initialize: subtract column minima
		if (square) {
			for (int s = 0; s < sLen; s++) {
				if (colMin[s] == 0) // there will be no change in this column
					continue;
				for (int r = 0; r < rLen; r++) {
					pi[r][s] = pi[r][s] - colMin[s];
				}
			}
		}
		initPiTime += System.nanoTime() - localStartTime;

		localStartTime = System.nanoTime();
		double sumMatching = 0;
		if (alg == 0)
			sumMatching = findMatching(pi, add, hits2);
		else if (alg == 1)
			sumMatching = findMatchingUB(pi, add, hits2, nnEdges, persThreshold);
		else if (alg == 2)
			sumMatching = findMatchingULB(pi, add, hits2, nnEdges, persThreshold, pq);
		matchingTime += System.nanoTime() - localStartTime;
		return sumMatching / (originalRLen + originalSLen - sumMatching);

	}

	/**
	 * Method to calculate Maximum Weighted Bipartite Matching (Edit).
	 * 
	 * @param R:             Strings of left Partition.
	 * @param S:             Strings of right Partition.
	 * @param hits:          Array of element scores. Depending on the algorithm,
	 *                       not all scores could have been calculated before.
	 * @param clusteringR:   Clustering of left Partition for element deduplication.
	 * @param clusteringS:   Clustering of right Partition for element
	 *                       deduplication.
	 * @param persThreshold: θ for this specific pair: θ = δ / (1+δ) * (|R|+|S|)
	 * @param alg:           Algorithm to perform: 0 is classic Hungarian Algorithm,
	 *                       1 is for Hungarian + Upper Bounds and 2 is for
	 *                       Hungarian + Upper {@literal &} Lower Bounds.
	 * @return Record Similarity based on matching m, i.e. m / (|R|+|S|-m).
	 */
	@SuppressWarnings("unchecked")
	public double verifyGraph(String[] R, String[] S, double[][] hits, Clustering[] clusteringR,
			Clustering[] clusteringS, double persThreshold, int alg) {
		long localStartTime = System.nanoTime();
		TIntSet rVertices = new TIntHashSet();
		TIntSet sVertices = new TIntHashSet();

		double add = deduplicateGraph(clusteringR, clusteringS, rVertices, sVertices);
		int originalRLen = R.length;
		int originalSLen = S.length;
		dedupTime += System.nanoTime() - localStartTime;

		if (alg == 2 && add - persThreshold > 0.0000001) {
			LBSaved1++;
			return add / (originalRLen + originalSLen - add);
		}

		int rLen = rVertices.size();
		int sLen = sVertices.size();

		if (rLen == 0) { // no non-identical left nodes are left for matching
			return add / (originalRLen + originalSLen - add);
		}

		localStartTime = System.nanoTime();
		double[] colMin = new double[sLen];
		boolean square = false;
		if (rLen == sLen) { // square matrix
			for (int s = 0; s < sLen; s++)
				colMin[s] = 1.0;
			square = true;
		} else {
			rLen = sLen; // square matrix
			square = false;
		}

		double[][] hits2 = new double[rLen][];
		for (int r = 0; r < hits2.length; r++)
			hits2[r] = new double[sLen];

		TIntIterator rit, sit;
		double[] nnEdges = new double[rLen];

		PriorityQueue<Element>[] pq = null;
		if (alg == 2)
			pq = new PriorityQueue[rLen];

		double UB = add + rVertices.size();
		rit = rVertices.iterator();
		int ri = 0;
		while (rit.hasNext()) {
			int r = rit.next();

			if (alg == 2)
				pq[ri] = new PriorityQueue<Element>();

			sit = sVertices.iterator();
			int si = 0;
			while (sit.hasNext()) {
				int s = sit.next();
				if (hits != null && hits[r] != null && hits[r][s] > 0.0)
					hits2[ri][si] = hits[r][s];
				else
					hits2[ri][si] = Verification.verifyWithScore(R[r], S[s]);

				nnEdges[ri] = Math.max(nnEdges[ri], hits2[ri][si]);
				if (alg == 2)
					pq[ri].add(new Element(si, hits2[ri][si]));

				si++;
			}
//			if (alg > 0) { // UB & LB
				UB -= 1 - nnEdges[ri];
				if (persThreshold - UB > 0.0000001) {
					UBSaved1++;
					initHitsTime += System.nanoTime() - localStartTime;
					return UB / (originalRLen + originalSLen - UB);
				}
//			}
			ri++;
		}
		initHitsTime += System.nanoTime() - localStartTime;

		localStartTime = System.nanoTime();
		double[][] pi = new double[rLen][];

		// initialize: inverse and subtract row minima
		for (int r = 0; r < rLen; r++) {
			pi[r] = new double[sLen];
			for (int s = 0; s < sLen; s++) {
				pi[r][s] = nnEdges[r] - hits2[r][s];
				if (square)
					colMin[s] = Math.min(colMin[s], pi[r][s]);
			}

		}

		// initialize: subtract column minima
		if (square) {
			for (int s = 0; s < sLen; s++) {
				if (colMin[s] == 0) // there will be no change in this column
					continue;
				for (int r = 0; r < rLen; r++) {
					pi[r][s] = pi[r][s] - colMin[s];
				}
			}
		}
		initPiTime += System.nanoTime() - localStartTime;

		localStartTime = System.nanoTime();
		double sumMatching = 0;
		if (alg == 0)
			sumMatching = findMatching(pi, add, hits2);
		else if (alg == 1)
			sumMatching = findMatchingUB(pi, add, hits2, nnEdges, persThreshold);
		else if (alg == 2)
			sumMatching = findMatchingULB(pi, add, hits2, nnEdges, persThreshold, pq);
		matchingTime += System.nanoTime() - localStartTime;
		return sumMatching / (originalRLen + originalSLen - sumMatching);

	}

	class Graph {

		private int V; // No. of vertices
		public TIntSet adj[]; // Adjacency Lists
		private int ROffset, SOffset, totalOffset;
		private int src, dest;

		public Graph(double[][] e, int rLen, int sLen) {
			ROffset = 0;
			SOffset = rLen + ROffset;
			totalOffset = sLen + SOffset;

			V = rLen + sLen + 2; // R + S + (T, S)
			adj = new TIntSet[V];

			dest = totalOffset;
			src = totalOffset + 1;

			adj[dest] = new TIntHashSet();
			adj[src] = new TIntHashSet();
			for (int r = 0; r < rLen; r++) {
				adj[r + ROffset] = new TIntHashSet();
				adj[src].add(r + ROffset);
				for (int s = 0; s < sLen; s++) {
					if (e[r][s] == 0.0) {
						adj[r + ROffset].add(s + SOffset);
					}
					if (adj[s + SOffset] == null) {
						adj[s + SOffset] = new TIntHashSet();
						adj[s + SOffset].add(dest);
					}

				}
			}

//			for (int v = 0; v < SOffset; v++) {
//				adj[src].add(v);
//			}
//
//			for (int v = SOffset; v < totalOffset; v++) {
//				adj[v] = new TIntHashSet();
//			}
		}

		public void addEdge(int r, int s) {
			adj[r + ROffset].add(s + SOffset);
		}

		public void removeEdge(int r, int s) {
			adj[r + ROffset].remove(s + SOffset);
			adj[s + SOffset].remove(r + ROffset); // could be in a matching already
		}

		public void updateEdge(Edge e) {
			adj[src].remove(e.src);
			adj[e.src].remove(e.dest);
			adj[e.dest].add(e.src);
			adj[e.dest].remove(dest);
		}

		public void revertEdge(Edge e) {
			adj[src].add(e.src);
			adj[e.src].add(e.dest);
			adj[e.dest].remove(e.src);
			adj[e.dest].add(dest);
		}

		// a modified version of BFS that stores predecessor
		// of each vertex in array pred
		private int BFS(int src, int dest, int pred[], boolean[] visited) {
			TIntList queue = new TIntArrayList();

			// initially all vertices are unvisited
			// so v[i] for all i is false
			// and as no path is yet constructed
			for (int i = 0; i < V; i++) {
				pred[i] = -1;
			}

			// now source is first to be visited and
			// distance from source to itself should be 0
			visited[src] = true;
			queue.add(src);

			int u = -1, v = -1;
			// bfs Algorithm
			while (!queue.isEmpty()) {
				u = queue.removeAt(0);

				TIntIterator it = adj[u].iterator();
				while (it.hasNext()) {
					v = it.next();
					if (visited[v] == false) {
						visited[v] = true;
						pred[v] = u;
//						path.add(new Edge(u, v);
						queue.add(v);

						// stopping condition (when we find our destination)
						if (v == dest)
							return v;
					}
				}
			}

			if (u < SOffset) // landed in Left Partite, we have a collision
				return u;
			return -1;
		}

	}

	class Edge {
		int src;
		int dest;

		public Edge(int src, int dest) {
			this.src = Math.min(src, dest);
			this.dest = Math.max(src, dest);
		}

		@Override
		public String toString() {
			return String.format("(%d, %d)", src, dest);
		}

		@Override
		public int hashCode() {
			return src * 31 + dest;
		}

		@Override
		public boolean equals(Object o) {
			return (this.src == ((Edge) o).src && this.dest == ((Edge) o).dest);
		}
	}

	class Element implements Comparable<Element> {
		public int s;
		public double score;

		public Element(int s, double score) {
			this.s = s;
			this.score = score;
		}

		@Override
		public int compareTo(Element o) {
			return Double.compare(o.score, score); // descending
		}

		@Override
		public String toString() {
			return String.format("(%d, %.2f)", s, score);
		}
	}

	public static void initTerms() {
		UBSaved1 = 0;
		LBSaved1 = 0;
		UBSaved2 = 0;
		LBSaved2 = 0;

		dedupTime = 0;
		initHitsTime = 0;
		initPiTime = 0;
		matchingTime = 0;
	}

	@SuppressWarnings("unchecked")
	public static void writeTerms(JSONObject log, double verificationTime) {
		JSONObject times = new JSONObject();
		times.put("total", verificationTime / 1000000000.0);
		times.put("deduplication", GraphVerifier.dedupTime / 1000000000.0);
		times.put("init_hits", GraphVerifier.initHitsTime / 1000000000.0);
		times.put("init_pi", GraphVerifier.initPiTime / 1000000000.0);
		times.put("matching", GraphVerifier.matchingTime / 1000000000.0);
		log.put("verification_times", times);

		JSONObject terms = new JSONObject();
		terms.put("UBS1", GraphVerifier.UBSaved1);
		terms.put("LBS1", GraphVerifier.LBSaved1);
		terms.put("UBS2", GraphVerifier.UBSaved2);
		terms.put("LBS2", GraphVerifier.LBSaved2);
		log.put("verification_terms", terms);
	}

}