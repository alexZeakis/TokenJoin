package eu.smartdatalake.simjoin.alg.topk.jaccard;

import java.util.Arrays;
import java.util.PriorityQueue;

import org.json.simple.JSONObject;

import eu.smartdatalake.runners.TopkCompetitor;
import eu.smartdatalake.simjoin.alg.Algorithm;
import eu.smartdatalake.simjoin.alg.topk.Cand;
import eu.smartdatalake.simjoin.alg.topk.KPair;
import eu.smartdatalake.simjoin.util.GraphVerifier;
import eu.smartdatalake.simjoin.util.ProgressBar;
import eu.smartdatalake.simjoin.util.collection.FuzzyIntSetCollection;
import eu.smartdatalake.simjoin.util.index.jaccard.FuzzySetIndex;
import eu.smartdatalake.simjoin.util.index.jaccard.IndexTokenScore;
import eu.smartdatalake.simjoin.util.record.RecordTokenScore;
import eu.smartdatalake.simjoin.util.record.jaccard.TJRecordInfo;
import gnu.trove.iterator.TIntDoubleIterator;
import gnu.trove.iterator.TIntObjectIterator;
import gnu.trove.map.TIntDoubleMap;
import gnu.trove.map.hash.TIntDoubleHashMap;
import gnu.trove.set.TIntSet;
import gnu.trove.set.hash.TIntHashSet;

/**
 * TokenJoin method for TopK.
 *
 */
public class TokenJoin extends Algorithm {

	String method;
	long firstHalfTime, secondHalfTime;
	double deltaGeneration, mu, samplePercentage;
	int lambda, choice;

	public TokenJoin(TopkCompetitor c) {
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

		System.out.println(
				String.format("\n%s - (%s,%s,%s)", method, deltaGeneration, mu, lambda));

		joinTime = System.nanoTime();

		/* INDEX BUILDING */
		indexTime = System.nanoTime();
		FuzzySetIndex idx = new FuzzySetIndex();
		idx.buildIndex(collection);
		indexTime = System.nanoTime() - indexTime;

		/* EXECUTE THE JOIN ALGORITHM */
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
		ProgressBar pb = new ProgressBar(collection.sets.length);
		
		TIntDoubleMap cands = new TIntDoubleHashMap();

		for (int R = 0; R < collection.sets.length; R++) {

			// progress bar
			pb.progressK(joinTime, threshold);

			TJRecordInfo querySet = new TJRecordInfo(R, collection.sets[R], idx.lengths, idx.idx[R], threshold, true,
					true);

			TIntSet localRejected = cRejected[R];
			if (localRejected == null) {
				localRejected = new TIntHashSet();
				TIntSet duplicates = collection.hashGroups.get(collection.hashCodes[R]);
				if (duplicates != null) {
					localRejected.addAll(duplicates);
				}
			}

			int posTok = 0;

			int recLength = collection.sets[R].length;
			int recMaxLength = (int) Math.floor(recLength / threshold);

			/* CANDIDATE GENERATION */
			for (posTok = 0; posTok < querySet.tokens.length; posTok++) {
				RecordTokenScore tokenScore = querySet.tokens[posTok];
				int token = tokenScore.id;
				double zetUtilScore = tokenScore.utility;

				int true_min = Arrays.binarySearch(idx.lengths[token], R); // true_min is > 0, since R is in
																			// tokenList
				int[] tempCands = idx.lengths[token];
				int tempLen = tempCands.length;

				if (querySet.theta - querySet.sumStopped > 0.0000001)
					break;

				querySet.sumStopped -= zetUtilScore;

				for (int Si = true_min; Si < tempLen; Si++) {
					int S = tempCands[Si];
					if (R == S)
						continue;
					if (collection.sets[S].length > recMaxLength) {
						break;
					}
					if (localRejected.contains(S))
						continue;

					cands.adjustOrPutValue(S, zetUtilScore, zetUtilScore);
				}
			}

			/* CANDIDATE PRIORITIZATION */
			PriorityQueue<Cand> Q = new PriorityQueue<Cand>();
			TIntDoubleIterator cit = cands.iterator();
			while (cit.hasNext()) {
				cit.advance();

				double total = querySet.sumStopped + cit.value();
				double score = total / (collection.sets[R].length + collection.sets[cit.key()].length - total);

				if (threshold - score > .0000001) { // Zombie Kill to avoid inserting in Q
					continue;
				}

				Q.add(new Cand(cit.key(), cit.value(), score));
			}

			/* CANDIDATE REFINEMENT */
			postLoop: while (!Q.isEmpty() && Q.peek().score > threshold) {
				Cand c = Q.poll();
				int S = c.id;

				double persThreshold = threshold / (1.0 + threshold)
						* (collection.sets[R].length + collection.sets[S].length);

				if (c.stage == 0) { // after Phase 1 and before Post - θ
					double utilGathered = c.utilGathered;
					double UBR = querySet.sumStopped;
					if (persThreshold - (UBR + utilGathered) > .0000001) { // Zombie Kill with possibly new
																			// threshold
						continue postLoop;
					}

					double restUB = UBR;
					for (int t = posTok; t < querySet.tokens.length; t++) {
						RecordTokenScore tokenScore = querySet.tokens[t];
						int token = tokenScore.id;
						double zetUtilScore = tokenScore.utility;

						UBR -= tokenScore.utility;
						IndexTokenScore tokS = idx.idx[S].get(token);
						restUB = UBR;
						if (tokS != null) {
							utilGathered += zetUtilScore;
							restUB = Math.min(restUB, tokS.rest);
						}

						if (persThreshold - (restUB + utilGathered) > .0000001) {
							continue postLoop;
						}
					}

					c.utilGathered = utilGathered + UBR;
					c.score = c.utilGathered / (collection.sets[R].length + collection.sets[S].length - c.utilGathered);
					c.stage++;
					Q.add(c);
				} else if (c.stage == 1) { // after Post-θ and before Joint
					double total = c.utilGathered;
					if (persThreshold - total > .0000001) { // Zombie Kill with possibly new threshold
						continue postLoop;
					}

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
							continue postLoop;
						}
					}

					c.utilGathered = total;
					c.score = c.utilGathered / (collection.sets[R].length + collection.sets[S].length - c.utilGathered);
					c.stage++;
					Q.add(c);
				} else { // after Joint and before Verification

					/* VERIFICATION */
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
						B.poll();
						threshold = B.peek().score;
					}
				}
			}

			if ((System.nanoTime() - joinTime) / 1000000000.0 > timeOut) { // more than 5 hours
				log.put("percentage", 1.0 * R / collection.sets.length);
				break;
			}
			
			cands.clear();
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
}