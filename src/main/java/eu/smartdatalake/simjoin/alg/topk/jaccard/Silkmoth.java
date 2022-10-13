package eu.smartdatalake.simjoin.alg.topk.jaccard;

import java.util.Arrays;
import java.util.PriorityQueue;

import org.json.simple.JSONObject;

import eu.smartdatalake.runners.TopkCompetitor;
import eu.smartdatalake.simjoin.alg.Algorithm;
import eu.smartdatalake.simjoin.alg.topk.KPair;
import eu.smartdatalake.simjoin.util.GraphVerifier;
import eu.smartdatalake.simjoin.util.ProgressBar;
import eu.smartdatalake.simjoin.util.Verification;
import eu.smartdatalake.simjoin.util.collection.FuzzyIntSetCollection;
import eu.smartdatalake.simjoin.util.index.jaccard.FuzzySetIndex;
import eu.smartdatalake.simjoin.util.index.jaccard.FuzzySetIndex2;
import eu.smartdatalake.simjoin.util.index.jaccard.IndexTokenScore2;
import eu.smartdatalake.simjoin.util.record.jaccard.SMRecordInfo;
import gnu.trove.iterator.TIntDoubleIterator;
import gnu.trove.iterator.TIntIterator;
import gnu.trove.map.TIntDoubleMap;
import gnu.trove.map.hash.TIntDoubleHashMap;
import gnu.trove.set.TIntSet;
import gnu.trove.set.hash.TIntHashSet;

/**
 * Adapted Silkmoth for Topk. For Jaccard Similarity.
 *
 */
public class Silkmoth extends Algorithm {
	String method;
	long firstHalfTime, secondHalfTime;
	double deltaGeneration, mu, samplePercentage;
	int lambda, choice;

	public Silkmoth(TopkCompetitor c) {
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

		System.out.println("\nSilkmoth");

		joinTime = System.nanoTime();

		/* INDEX BUILDING */
		indexTime = System.nanoTime();
		FuzzySetIndex2 idx = new FuzzySetIndex2(collection);
		FuzzySetIndex idx2 = new FuzzySetIndex(collection);
		indexTime = System.nanoTime() - indexTime;

		/* BUCKET INITIALIZATION */
		initTime = System.nanoTime();
		PriorityQueue<KPair> B = new PriorityQueue<KPair>();
		TIntSet[] cRejected = new TIntSet[collection.sets.length];
		initTime = System.nanoTime() - initTime;

		/* THRESHOLD INITIALIZATION */
		firstHalfTime = System.nanoTime();
		double threshold = new ThresholdInitializer(deltaGeneration, mu, lambda, samplePercentage).initThreshold(choice,
				k, collection, idx2, B, cRejected, joinTime);
		double initThreshold = threshold;
		System.out.println("Init Threshold: " + initThreshold);

		firstHalfTime = System.nanoTime() - firstHalfTime;

		secondHalfTime = System.nanoTime();
		/* EXECUTE THE JOIN ALGORITHM */

		int maxRecLength = collection.sets[collection.sets.length - 1].length;
		double[][] hits = new double[maxRecLength][];
		for (int nor = 0; nor < maxRecLength; nor++)
			hits[nor] = new double[maxRecLength];
		boolean[] matchedElements = new boolean[maxRecLength];
		boolean[] rejected = new boolean[collection.sets.length];
		TIntSet cands = new TIntHashSet();
		TIntDoubleMap[] candsElements = new TIntDoubleMap[collection.sets.length];
		for (int S = 0; S < collection.sets.length; S++)
			candsElements[S] = new TIntDoubleHashMap();
		ProgressBar pb = new ProgressBar(collection.sets.length);

		for (int R = 0; R < collection.sets.length; R++) {
			// progress bar
			pb.progressK(joinTime, threshold);

			/* SIGNATURE GENERATION */
			SMRecordInfo querySet = new SMRecordInfo(R, collection.sets[R], idx, false);
			querySet.computeUnflattenedSignature(idx, threshold, true, collection.sets[R]);

			// compute bounds for length filter
			int recLength = collection.sets[R].length;
			int recMaxLength = (int) Math.floor(collection.sets[R].length / threshold);

			TIntSet duplicates = collection.hashGroups.get(collection.hashCodes[R]);
			if (duplicates != null) {
				TIntIterator it = duplicates.iterator();
				while (it.hasNext()) {
					rejected[it.next()] = true;
				}
			}

			if (cRejected[R] != null) {
				TIntIterator it = cRejected[R].iterator();
				while (it.hasNext()) {
					rejected[it.next()] = true;
				}
			}

			/* CANDIDATE GENERATION */
			for (int r = 0; r < querySet.unflattenedSignature.length; r++) {
				for (int t = 0; t < querySet.unflattenedSignature[r].size(); t++) {
					int token = querySet.unflattenedSignature[r].get(t);

					int true_min2 = Arrays.binarySearch(idx.recordIndex[token], R); // true_min is > 0, since i is in
																					// tokenList
					int[] recordCands = idx.recordIndex[token];
					int[] elementCands = idx.elementIndex[token];
					int candsSize = recordCands.length;

					while (true_min2 > 0 && recordCands[true_min2] == recordCands[true_min2 - 1]) // in case of multiple
																									// occasions of S
						true_min2--;

					for (int Si = true_min2; Si < candsSize; Si++) {
						int S = recordCands[Si];

						if (rejected[S])
							continue;

						int s = elementCands[Si];
						if (R == S)
							continue;
						if (collection.sets[S].length > recMaxLength) {
							break;
						}

						double score = Verification.verifyWithScore(collection.sets[R][r], collection.sets[S][s]);
						if (score >= querySet.elementBounds[r]) {
							cands.add(S);
							double val = candsElements[S].get(r);
							if (val == candsElements[S].getNoEntryValue() || score > val)
								candsElements[S].put(r, score);
						}
					}
				}
			}

			PriorityQueue<SMCand> Q = new PriorityQueue<SMCand>();
			TIntIterator cit = cands.iterator();
			while (cit.hasNext()) {
				int S = cit.next();
				int candLength = collection.sets[S].length;

				double total = recLength;
				TIntDoubleIterator it3 = candsElements[S].iterator();
				while (it3.hasNext()) {
					it3.advance();
					total -= 1.0;
					total += it3.value();
				}
//				double score = 1.0 * recLength / candLength;
				double score = total / (recLength + candLength - total);
				Q.add(new SMCand(S, collection.sets[R].length, score));
			}

			cands.clear();

			while (!Q.isEmpty() && Q.peek().score > threshold) {
				SMCand cm = Q.poll();
				int S = cm.id;
				int candLength = collection.sets[cm.id].length;

				if (cm.stage == 0) { // after Check Filter and before NNF
					/* NEAREST NEIGHBOR FILTER */
					double persThreshold = threshold / (1.0 + threshold) * (recLength + candLength);

					// INIT TOTALUB
					double totalUB = 0;
					for (int r = 0; r < recLength; r++) {
						totalUB += querySet.elementBounds[r];
						matchedElements[r] = false;
					}

					// INIT Elements from CF
					TIntDoubleIterator it3 = candsElements[S].iterator();
					while (it3.hasNext()) {
						it3.advance();
						int r = it3.key();
						matchedElements[r] = true;
						totalUB -= querySet.elementBounds[r];
						totalUB += it3.value();

						for (int nos = 0; nos < candLength; nos++)
							hits[r][nos] = 0.0;
					}
					candsElements[S].clear();

					// Find for Elements not in CF
					for (int r = 0; r < recLength; r++) {
						if (matchedElements[r]) {
							continue;
						}

						for (int nos = 0; nos < candLength; nos++)
							hits[r][nos] = 0.0;

						totalUB -= querySet.elementBounds[r];

						double maxSim = 0.0;
						double UBStep = 1.0 / collection.sets[R][r].length;
						double elemUB = 1.0;
						for (int token : collection.sets[R][r]) {
							elemUB -= UBStep;

							IndexTokenScore2 tok = idx.idx[S].get(token);
							if (tok == null)
								continue;
							if (tok != null) {
								for (int s : tok.elements) {
									if (hits[r][s] == 0.0) {
										hits[r][s] = Verification.verifyWithScore(collection.sets[R][r],
												collection.sets[S][s]);
									}
									maxSim = Math.max(maxSim, hits[r][s]);

								}
							}
							if (maxSim - elemUB >= 0.00000001)
								break;
						}

						totalUB += maxSim;

						if (persThreshold - totalUB > 0.000000001) {
							break;
						}
					}

					cm.score = totalUB / (recLength + candLength - totalUB);
					if (cm.score - threshold > 0.000000001) {
						cm.stage++;
						Q.add(cm);
					}
				} else { // after NNF and before Verification
					/* VERIFICATION */
					double persThreshold = threshold / (1.0 + threshold) * (recLength + candLength);
					GraphVerifier eval4 = new GraphVerifier();
					double score = eval4.verifyGraph(collection.sets[R], collection.sets[S], cm.hits,
							collection.getClustering(R), collection.getClustering(S), persThreshold, 0);
					if (score == 1.0)
						continue;

					if (threshold - score > 0.000000001)
						continue;
					B.add(new KPair(R, S, score));
					if (B.size() > k) {
						B.poll();
						if (B.peek().score != threshold) {
							threshold = B.peek().score;
							querySet.computeUnflattenedSignature(idx, threshold, true, collection.sets[R]);
						}
					}
				}
			}
			
			while (!Q.isEmpty()) {
				candsElements[Q.poll().id].clear();
			}

			if ((System.nanoTime() - joinTime) / 1000000000.0 > timeOut) { // more than 5 hours
				log.put("percentage", 1.0 * R / collection.sets.length);
				break;
			}

			if (duplicates != null) {
				TIntIterator it = duplicates.iterator();
				while (it.hasNext()) {
					rejected[it.next()] = false;
				}
			}

			if (cRejected[R] != null) {
				TIntIterator it = cRejected[R].iterator();
				while (it.hasNext()) {
					rejected[it.next()] = false;
				}
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
		terms.put("final_threshold", threshold);
		terms.put("init_threshold", initThreshold);
		log.put("terms", terms);
		logger.info(log.toString());

		System.out.println();
		System.out.println("Join time: " + joinTime / 1000000000.0 + " sec.");
		System.out.println("Final Threshold: " + threshold);
		System.out.println("Number of matches: " + B.size());

//		while (!B.isEmpty())
//			logger.info(B.poll());

		return threshold;

	}

	class SMCand implements Comparable<SMCand> {
		public int id;
		public int stage;
		public double score;
		public double[][] hits;
		public double[] nearestNeighborSim;

		public SMCand(int id, int recLength, double score) {
			this.id = id;
			this.hits = new double[recLength][];
			nearestNeighborSim = new double[recLength];
			this.score = score;
			this.stage = 0;
		}

		@Override
		public String toString() {
			return String.format("%s->(%s,%d)", id, score, stage);
		}

		@Override
		public int compareTo(SMCand o) {
			if (this.score == o.score) {
				return Integer.compare(this.id, o.id);
			}
			return Double.compare(o.score, this.score); // descending order
		}

	}
}