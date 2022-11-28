package gr.athenarc.imsi.simjoin.alg.topk.edit;

import java.util.Arrays;
import java.util.PriorityQueue;

import org.json.simple.JSONObject;

import gnu.trove.iterator.TIntIterator;
import gnu.trove.list.TIntList;
import gnu.trove.list.array.TIntArrayList;
import gnu.trove.set.TIntSet;
import gr.athenarc.imsi.simjoin.alg.Algorithm;
import gr.athenarc.imsi.simjoin.alg.topk.KPair;
import gr.athenarc.imsi.simjoin.util.GraphVerifier;
import gr.athenarc.imsi.simjoin.util.collection.FuzzyIntSetCollection;
import gr.athenarc.imsi.simjoin.util.index.edit.FuzzySetIndex;
import gr.athenarc.imsi.simjoin.util.index.edit.IndexTokenScore;
import gr.athenarc.imsi.simjoin.util.record.RecordTokenScore;
import gr.athenarc.imsi.simjoin.util.record.edit.TJRecordInfo;

/**
 * Baseline methods for Topk. For Edit Similarity.
 *
 */
public class Fixed extends Algorithm {

	public Fixed() {
		super();
		log = new JSONObject();
	}

	/**
	 * Method that starts from an initial δ=1-α and reduces δ by α, until k results are found.
	 * @param collection: Collection of records
	 * @param k: Number of pairs.
	 * @param alpha: Ratio to reduce δ
	 * @return final δ
	 */
	@SuppressWarnings("unchecked")
	public double selfJoinAlpha(FuzzyIntSetCollection collection, int k, double alpha) {

		System.out.println(String.format("\nNaiveAlpha with alpha: %s", alpha));

		joinTime = System.nanoTime();

		FuzzySetIndex idx = new FuzzySetIndex(collection);

		PriorityQueue<KPair> B = null;
		double threshold = 0.0;
		for (threshold = 1 - alpha; threshold > 0; threshold -= alpha) {
			B = new PriorityQueue<KPair>();
			System.out.print(String.format("%.2f ", threshold));

			runThresholdJoin(collection, idx, threshold, B);
			if (B.size() >= k)
				break;
		}

		joinTime = System.nanoTime() - joinTime;

		while (B.size() > k)
			B.poll();
		threshold = B.peek().score;

		log.put("name", "NaiveAlpha");
		log.put("size", collection.sets.length);
		log.put("k", k);
		JSONObject times = new JSONObject();
		times.put("total", joinTime/ 1000000000.0);
		log.put("times", times);
		JSONObject terms = new JSONObject();
		terms.put("final_threshold", threshold);
		log.put("terms", terms);
		logger.info(log.toString());
		
		System.out.println();
		System.out.println("Join time: " + joinTime / 1000000000.0 + " sec.");
		System.out.println("Final Threshold: " + threshold);
		System.out.println("Number of matches: " + B.size());

		return threshold;
	}

	/**
	 * Method that runs a thresholdJoin with the final δ to find optimal time.
	 * @param collection: Collection of records
	 * @param k: Number of pairs.
	 * @param threshold: Final δ
	 * @return final δ
	 */
	@SuppressWarnings("unchecked")
	public double selfJoinFixed(FuzzyIntSetCollection collection, int k, double threshold) {

		System.out.println(String.format("\nNaiveThreshold with threshold: %s", threshold));

		joinTime = System.nanoTime();

		FuzzySetIndex idx = new FuzzySetIndex(collection);

		PriorityQueue<KPair> B = new PriorityQueue<KPair>();
		
		runThresholdJoin(collection, idx, threshold, B);

		joinTime = System.nanoTime() - joinTime;

		log.put("name", "OT");
		log.put("size", collection.sets.length);
		log.put("k", k);
		JSONObject times = new JSONObject();
		times.put("total", joinTime/ 1000000000.0);
		log.put("times", times);
		JSONObject terms = new JSONObject();
		terms.put("final_threshold", threshold);
		log.put("terms", terms);
		logger.info(log.toString());
		
		
		System.out.println();
		System.out.println("Join time: " + joinTime / 1000000000.0 + " sec.");
		System.out.println("Final Threshold: " + threshold);
		System.out.println("Number of matches: " + B.size());

		return threshold;
	}

	protected double postOpt(int R, int S, TJRecordInfo querySet, FuzzySetIndex idx, double persThreshold,
			double utilGathered, double UBR, int posTok) {

		double restUB = UBR;
		for (int t = posTok; t < querySet.tokens.length; t++) {
			RecordTokenScore tokenScore = querySet.tokens[t];
			int token = tokenScore.id;

			UBR -= tokenScore.utility;
			restUB = UBR;
			IndexTokenScore tokS = idx.idx[S].get(token);
			if (tokS != null) {

				IndexTokenScore tokR = idx.idx[R].get(token);
				int len = Math.min(tokR.utilities2.length, tokS.utilities2.length) - 1;
				double utilScore = Math.min(tokR.utilities2[len], tokS.utilities2[len]);
				utilGathered += utilScore;

				restUB = Math.min(UBR, tokS.rest);
			}

			if (persThreshold - (utilGathered + restUB) > .0000001)
				return (utilGathered + restUB);
		}
		return (utilGathered + restUB);
	}
	
	@SuppressWarnings("unchecked")
	protected void runThresholdJoin(FuzzyIntSetCollection collection, FuzzySetIndex idx, double threshold, PriorityQueue<KPair> B) {
		/* EXECUTE THE JOIN ALGORITHM */
		collection.clearClusterings();
		initVerificationTerms();
		
		TIntList cands = new TIntArrayList();
		double[] candsScores = new double[collection.sets.length];

		for (int R = 0; R < collection.sets.length; R++) {
			TJRecordInfo querySet = new TJRecordInfo(R, collection.sets[R], collection.qsets[R], idx,
					threshold, true, true);

			int recLength = collection.sets[R].length;
			int recMaxLength = (int) Math.floor(recLength / threshold);

			TIntSet duplicates = collection.hashGroups.get(collection.hashCodes[R]);
			if (duplicates != null) {
				TIntIterator it = duplicates.iterator();
				while (it.hasNext()) {
					candsScores[it.next()] = -1;
				}
			}

			int posTok = 0;
			for (posTok = 0; posTok < querySet.tokens.length; posTok++) {
				RecordTokenScore tokenScore = querySet.tokens[posTok];
				int token = tokenScore.id;
				double zetUtilScore = tokenScore.utility;

				int true_min = Arrays.binarySearch(idx.lengths[token], R); // true_min is > 0, since R is in
																			// tokenList
				int[] tempCands = idx.lengths[token];
				int tempLen = tempCands.length;

				if (querySet.sumStopped < querySet.theta)
					break;

				querySet.sumStopped -= zetUtilScore;

				for (int Si = true_min; Si < tempLen; Si++) {
					int S = tempCands[Si];
					if (R == S)
						continue;
					if (collection.sets[S].length > recMaxLength) {
						break;
					}
					if (candsScores[S] == -1) // duplicate or verified
						continue;

					if (candsScores[S] == 0)
						cands.add(S);
					candsScores[S] += zetUtilScore;
				}
			}

			TIntIterator it = cands.iterator();
			while (it.hasNext()) {
				int S = it.next();
				double utilGathered = candsScores[S];
				candsScores[S] = 0;
				int candLength = collection.sets[S].length;

				double persThreshold = threshold / (1.0 + threshold) * (recLength + candLength);

				double total = querySet.sumStopped + utilGathered;
				if (persThreshold - total > .0000001)
					continue;

				double UB = postOpt(R, S, querySet, idx, persThreshold, utilGathered, querySet.sumStopped, posTok);

				if (persThreshold - UB > 0.000000001)
					continue;

				/* VERIFICATION */
				GraphVerifier eval4 = new GraphVerifier();

				double score = eval4.verifyGraph(collection.originalStrings[R], collection.originalStrings[S], null,
						collection.getClustering(R), collection.getClustering(S), persThreshold, 1);

				if (score == 1.0)
					continue;

				if (threshold - score > 0.000000001)
					continue;
				B.add(new KPair(R, S, score));
			}

			if ((System.nanoTime() - joinTime) / 1000000000.0 > timeOut) { // more than 5 hours
				log.put("percentage", 1.0 * R / collection.sets.length);
				break;
			}		
			
			if (duplicates != null) {
				it = duplicates.iterator();
				while (it.hasNext()) {
					candsScores[it.next()] = 0;
				}
			}
			cands.clear();
			
		}
	}

}