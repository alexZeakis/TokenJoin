package eu.smartdatalake.simjoin.alg.topk.jaccard;

import java.util.Arrays;
import java.util.PriorityQueue;
import java.util.TreeSet;

import org.json.simple.JSONObject;

import eu.smartdatalake.runners.TopkCompetitor;
import eu.smartdatalake.simjoin.alg.Algorithm;
import eu.smartdatalake.simjoin.alg.topk.KPair;
import eu.smartdatalake.simjoin.util.GraphVerifier;
import eu.smartdatalake.simjoin.util.collection.FuzzyIntSetCollection;
import eu.smartdatalake.simjoin.util.index.jaccard.FuzzySetIndex;
import eu.smartdatalake.simjoin.util.index.jaccard.IndexTokenScore;
import gnu.trove.iterator.TIntObjectIterator;
import gnu.trove.set.TIntSet;
import gnu.trove.set.hash.TIntHashSet;

/**
 * Adapted TopK-Join to solve Fuzzy TopK.
 *
 */
public class TopkJoin extends Algorithm {
	String method;
	long firstHalfTime, secondHalfTime;
	double deltaGeneration, mu, samplePercentage;
	int lambda, choice;

	public TopkJoin(TopkCompetitor c) {
		this.method = c.method;
		this.deltaGeneration = c.deltaGeneration;

		this.mu = c.mu;
		this.lambda = c.lambda;
		this.choice = c.choice;

		this.samplePercentage = c.samplePercentage;

		log = new JSONObject();
		c.write(log);
	}

	/**
	 * Method to perform Join.
	 * 
	 * @param collection: Collection of records
	 * @param k:          Number of pairs.
	 * @return final δ
	 */
	@SuppressWarnings("unchecked")
	public double selfJoin(FuzzyIntSetCollection collection, int k) {
		
		collection.clearClusterings();
		initVerificationTerms();

		System.out.println("\nTopk-Join");

		joinTime = System.nanoTime();

		/* INDEX BUILDING */
		indexTime = System.nanoTime();
		FuzzySetIndex idx = new FuzzySetIndex(collection);

		int[][] tokens = new int[collection.sets.length][];
		double[][] weights = new double[collection.sets.length][];
		makeSimpleIndex(idx, tokens, weights);
		indexTime = System.nanoTime() - indexTime;

		/* BUCKET INITIALIZATION */
		initTime = System.nanoTime();
		PriorityQueue<KPair> B2 = new PriorityQueue<KPair>();

		PriorityQueue<PrefixEvent> Q = new PriorityQueue<PrefixEvent>();
		TIntSet[] rejected = new TIntSet[collection.sets.length];
		for (int i = 0; i < collection.sets.length; i++) {
			Q.add(new PrefixEvent(i, collection.sets[i].length));
			rejected[i] = new TIntHashSet();
			TIntSet duplicates = collection.hashGroups.get(collection.hashCodes[i]);
			if (duplicates != null) {
				rejected[i].addAll(duplicates);
			}
		}
		initTime = System.nanoTime() - initTime;

		/* THRESHOLD INITIALIZATION */
		firstHalfTime = System.nanoTime();
		double threshold = new ThresholdInitializer(deltaGeneration, mu, lambda, samplePercentage).initThreshold(choice,
				k, collection, idx, B2, rejected, joinTime);
		double initThreshold = threshold;
		System.out.println("Init Threshold: " + initThreshold);

		firstHalfTime = System.nanoTime() - firstHalfTime;

		secondHalfTime = System.nanoTime();
		for (int i = 0; i < collection.sets.length; i++) {
			noReject += rejected[i].size();
		}
		
		TreeSet<KPair> B = new TreeSet<KPair>(B2);

		long noEvents = 0;
		while (!Q.isEmpty() && Q.peek().score >= threshold) {
			noEvents++;
			if (noEvents % 10000 == 0)
				System.out.print(String.format("No Events: %s, Cache: %s, δ: %s\r", noEvents, noReject, threshold));

			PrefixEvent pe = Q.poll();
			int R = pe.id;
			int token = tokens[R][pe.token];

			int true_min = Arrays.binarySearch(idx.lengths[token], R); // true_min is > 0, since R is in tokenList
			int[] tempCands = idx.lengths[token];
			int tempLen = tempCands.length;

			int recLength = collection.sets[R].length;
			int recMaxLength = (int) Math.floor(recLength / threshold);

			/* CANDIDATE GENERATION BASED ON THIS EVENT */
			for (int Si = true_min; Si < tempLen; Si++) {
				int S = tempCands[Si];
				if (R == S)
					continue;
				if (collection.sets[S].length > recMaxLength) {
					break;
				}
				if (rejected[R].contains(S))
					continue;

				if (noReject < LimitReject) {
					rejected[R].add(S);
					noReject++;
				}

				double persThreshold = threshold / (1.0 + threshold)
						* (collection.sets[R].length + collection.sets[S].length);

				/* POSITION FILTER */
				double UBR = pe.UB;
				double utilGathered = 0;
				for (int t = pe.token; t < tokens[R].length; t++) {
					int tok2 = tokens[R][t];
					double util2 = weights[R][t];

					UBR -= util2;
					double restUB = UBR;
					IndexTokenScore tokS = idx.idx[S].get(tok2);
					if (tokS != null) {
						utilGathered += util2;
						restUB = Math.min(UBR, tokS.rest);
					}

					if (persThreshold - (utilGathered + restUB) > .0000001) {
						break;
					}
				}

				/* JOINT UTILITY FILTER */
				double total = utilGathered + UBR;
				TIntObjectIterator<IndexTokenScore> it = idx.idx[R].iterator();
				while (it.hasNext()) {
					it.advance();
					IndexTokenScore tokR = it.value();
					IndexTokenScore tokS = idx.idx[S].get(tokR.id);
					if (tokS != null) {
						total -= tokR.value;

						int len = Math.min(tokR.utilities.length, tokS.utilities.length) - 1;
						double utilScore = Math.min(tokR.utilities[len], tokS.utilities[len]);
						total += utilScore;
					}

					if (persThreshold - total > .0000001) {
						break;
					}
				}

				/* VERIFICATION */
				if (total - persThreshold > .0000001) {

					GraphVerifier eval4 = new GraphVerifier();
					// Calling VUB -> 1
					double score = eval4.verifyGraph(collection.sets[R], collection.sets[S], null,
							collection.getClustering(R), collection.getClustering(S), persThreshold, 1);
					if (score == 1.0)
						continue;

					if (threshold - score > 0.000000001)
						continue;
					B.add(new KPair(R, S, score));
					if (B.size() > k) {
						B.pollFirst();
						threshold = B.first().score;
					}
					if (B.size() == k)
						threshold = B.first().score;
				}
			}

			pe.UB -= weights[R][pe.token];
			pe.token++;

			pe.score = 1.0 * pe.UB / recLength;

			if (pe.token < tokens[R].length)
				Q.add(pe);

			if ((System.nanoTime() - joinTime) / 1000000000.0 > timeOut) { // more than 5 hours
				log.put("percentage", 1.0 * R / collection.sets.length);
				break;
			}
		}

		secondHalfTime = System.nanoTime() - secondHalfTime;

		joinTime = System.nanoTime() - joinTime;

		log.put("name", method);
		log.put("size", collection.sets.length);
		log.put("k", k);
		log.put("final_threshold", threshold);
		JSONObject times = new JSONObject();
		times.put("total", joinTime / 1000000000.0);
		times.put("index", indexTime / 1000000000.0);
		times.put("init", initTime / 1000000000.0);
		times.put("first_half", firstHalfTime / 1000000000.0);
		times.put("second_half", secondHalfTime / 1000000000.0);
		log.put("times", times);
		JSONObject terms = new JSONObject();
		terms.put("noReject", noReject);
		terms.put("events", noEvents);
		terms.put("final_threshold", threshold);
		terms.put("init_threshold", initThreshold);
		log.put("terms", terms);
		logger.info(log.toString());

		System.out.println();
		System.out.println("Join time: " + joinTime / 1000000000.0 + " sec.");
		System.out.println("Final Threshold: " + threshold);
		System.out.println("Number of matches: " + B.size());

//		while(!B.isEmpty())
//			logger.info(B.poll());

		return threshold;
	}

	/**
	 * Method for constructing index of flattened sets, only for TopK-Join
	 * algorithm.
	 * 
	 * @param idx:     Original Index
	 * @param tokens:  Flattened set of tokens
	 * @param weights: Flattened set of weights
	 */
	void makeSimpleIndex(FuzzySetIndex idx, int[][] tokens, double[][] weights) {

		for (int i = 0; i < idx.idx.length; i++) {
			TIntObjectIterator<IndexTokenScore> it = idx.idx[i].iterator();

			SimpleToken[] array = new SimpleToken[idx.idx[i].size()];
			int j = 0;
			while (it.hasNext()) {
				it.advance();
				array[j++] = new SimpleToken(it.key(), it.value().value);
			}

			Arrays.sort(array);

			tokens[i] = new int[array.length];
			weights[i] = new double[array.length];
			for (j = 0; j < array.length; j++) {
				tokens[i][j] = array[j].token;
				weights[i][j] = array[j].weight;
			}

		}
	}

	class PrefixEvent implements Comparable<PrefixEvent> {
		public int id, token;
		public double UB, score;

		public PrefixEvent(int id, double UB) {
			this.id = id;
			this.UB = UB;
			this.score = 1.0;
			this.token = 0;
		}

		@Override
		public int compareTo(PrefixEvent o) {
			return Double.compare(o.score, this.score); // descending order
		}
	}

	class SimpleToken implements Comparable<SimpleToken> {
		public int token;
		public double weight;

		public SimpleToken(int token, double weight) {
			this.token = token;
			this.weight = weight;
		}

		@Override
		public int compareTo(SimpleToken o) {
			return Integer.compare(this.token, o.token); // ascending order
		}
	}
}