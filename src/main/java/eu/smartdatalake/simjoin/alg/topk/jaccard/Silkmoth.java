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
import eu.smartdatalake.simjoin.util.index.jaccard.IndexTokenScore;
import eu.smartdatalake.simjoin.util.record.jaccard.SMRecordInfo;
import gnu.trove.iterator.TIntIterator;
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
		FuzzySetIndex idx = new FuzzySetIndex();
		idx.buildIndex(collection);
		indexTime = System.nanoTime() - indexTime;

		/* BUCKET INITIALIZATION */
		initTime = System.nanoTime();
		PriorityQueue<KPair> B = new PriorityQueue<KPair>();
		TIntSet[] cRejected = new TIntSet[collection.sets.length];
		initTime = System.nanoTime() - initTime;

		/* THRESHOLD INITIALIZATION */
		firstHalfTime = System.nanoTime();
		double threshold = new ThresholdInitializer(deltaGeneration, mu, lambda, samplePercentage)
				.initThreshold(choice, k, collection, idx, B, cRejected, joinTime);
		double initThreshold = threshold;
		System.out.println("Init Threshold: " + initThreshold);

		firstHalfTime = System.nanoTime() - firstHalfTime;

		secondHalfTime = System.nanoTime();
		/* EXECUTE THE JOIN ALGORITHM */
		ProgressBar pb = new ProgressBar(collection.sets.length);

		for (int R = 0; R < collection.sets.length; R++) {
			// progress bar
			pb.progressK(joinTime, threshold);

			/* SIGNATURE GENERATION */
			SMRecordInfo querySet = new SMRecordInfo(R, collection.sets[R], idx.lengths, idx.idx[R], false);
			querySet.computeUnflattenedSignature(idx, threshold, true, collection.sets[R]);

			// compute bounds for length filter
			int recLength = collection.sets[R].length;
			int recMaxLength = (int) Math.floor(collection.sets[R].length / threshold);

			TIntSet localRejected = cRejected[R];
			if (localRejected == null) {
				localRejected = new TIntHashSet();
				TIntSet duplicates = collection.hashGroups.get(collection.hashCodes[R]);
				if (duplicates != null) {
					localRejected.addAll(duplicates);
				}
			}

			/* CANDIDATE GENERATION */
			TIntSet cands = new TIntHashSet();
			TIntIterator it = querySet.KTR.iterator();
			while (it.hasNext()) {
				int token = it.next();

				int true_min = Arrays.binarySearch(idx.lengths[token], R); // true_min is > 0, since i is in tokenList
				int[] tempCands = idx.lengths[token];
				int tempSize = tempCands.length;

				for (int Si = true_min; Si < tempSize; Si++) {
					int S = tempCands[Si];
					if (R == S)
						continue;
					if (collection.sets[S].length > recMaxLength) {
						break;
					}
					if (localRejected.contains(S))
						continue;

					cands.add(S);
				}
			}

			PriorityQueue<SMCand> Q = new PriorityQueue<SMCand>();
			TIntIterator cit = cands.iterator();
			while (cit.hasNext()) {
				int S = cit.next();
				int candLength = collection.sets[S].length;
				double score = 1.0 * recLength / candLength;
				Q.add(new SMCand(S, collection.sets[R].length, score));
			}

			while (!Q.isEmpty() && Q.peek().score > threshold) {
				SMCand cm = Q.poll();
				int S = cm.id;
				int candLength = collection.sets[cm.id].length;

				if (cm.stage == 0) { // after Phase 1 and before Check Filter
					/* CHECK FILTER */

					boolean pass = false;
					double totalUB = recLength;
					for (int r = 0; r < querySet.unflattenedSignature.length; r++) {
						cm.hits[r] = new double[candLength];
						double maxNN = 0.0;

						for (int t = 0; t < querySet.unflattenedSignature[r].size(); t++) {
							int token = querySet.unflattenedSignature[r].get(t);
							IndexTokenScore tok = idx.idx[S].get(token);
							if (tok == null)
								continue;
							for (int s : tok.elements) {
								cm.hits[r][s] = Verification.verifyWithScore(collection.sets[R][r],
										collection.sets[S][s]);
								if (cm.hits[r][s] >= querySet.elementBounds[r]) {
									pass = true;
								}
								maxNN = Math.max(cm.hits[r][s], maxNN);
							}
						}

						if (maxNN > 0) { // element had signature tokens
							cm.nearestNeighborSim[r] = maxNN;
							totalUB -= 1 - maxNN;
						}

					}

					if (pass) {
						cm.stage++;
						cm.score = totalUB / (recLength + candLength - totalUB);
						Q.add(cm);
					}
				} else if (cm.stage == 1) { // after Check Filter and before NNF
					/* NEAREST NEIGHBOR FILTER */
					double persThreshold = threshold / (1.0 + threshold) * (recLength + candLength);
					TIntSet matchedElements;

					double totalUB = 0;
					matchedElements = new TIntHashSet();
					for (int r = 0; r < collection.sets[R].length; r++) {
						if (cm.nearestNeighborSim[r] > querySet.elementBounds[r]) {
							matchedElements.add(r);
							totalUB += cm.nearestNeighborSim[r];
						} else {
							totalUB += querySet.elementBounds[r];
						}
					}

					for (int r = 0; r < recLength; r++) {
						if (matchedElements.contains(r)) {
							continue;
						}

						totalUB -= querySet.elementBounds[r];

						double maxSim = 0.0;
						double UBStep = 1.0 / collection.sets[R][r].length;
						double elemUB = 1.0;
						for (int token : collection.sets[R][r]) {
							elemUB -= UBStep;

							IndexTokenScore tok = idx.idx[cm.id].get(token);
							if (tok == null)
								continue;
							if (tok != null) {
								if (cm.hits[r] == null)
									cm.hits[r] = new double[candLength];
								for (int s : tok.elements) {
									if (cm.hits[r][s] == 0.0) {
										cm.hits[r][s] = Verification.verifyWithScore(collection.sets[R][r],
												collection.sets[S][s]);
									}
									maxSim = Math.max(maxSim, cm.hits[r][s]);

								}
							}
							if (maxSim - elemUB >= 0.00000001)
								break;
						}
						cm.nearestNeighborSim[r] = maxSim;

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