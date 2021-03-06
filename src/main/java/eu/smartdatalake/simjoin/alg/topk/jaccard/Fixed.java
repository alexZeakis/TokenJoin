package eu.smartdatalake.simjoin.alg.topk.jaccard;

import java.util.Arrays;
import java.util.PriorityQueue;

import org.json.simple.JSONObject;

import eu.smartdatalake.simjoin.alg.Algorithm;
import eu.smartdatalake.simjoin.alg.topk.KPair;
import eu.smartdatalake.simjoin.util.GraphVerifier;
import eu.smartdatalake.simjoin.util.collection.FuzzyIntSetCollection;
import eu.smartdatalake.simjoin.util.index.jaccard.FuzzySetIndex;
import eu.smartdatalake.simjoin.util.index.jaccard.IndexTokenScore;
import eu.smartdatalake.simjoin.util.record.RecordTokenScore;
import eu.smartdatalake.simjoin.util.record.jaccard.TJRecordInfo;
import gnu.trove.iterator.TIntDoubleIterator;
import gnu.trove.map.TIntDoubleMap;
import gnu.trove.map.hash.TIntDoubleHashMap;
import gnu.trove.set.TIntSet;
import gnu.trove.set.hash.TIntHashSet;

/**
 * Baseline methods for Topk. For Jaccard Similarity.
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

		FuzzySetIndex idx = new FuzzySetIndex();
		idx.buildIndex(collection);

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

		FuzzySetIndex idx = new FuzzySetIndex();
		idx.buildIndex(collection);

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
		
//		while (!B.isEmpty())
//		logger.info(B.poll());

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
				int len = Math.min(tokR.utilities.length, tokS.utilities.length) - 1;
				double utilScore = Math.min(tokR.utilities[len], tokS.utilities[len]);
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

		for (int R = 0; R < collection.sets.length; R++) {
			TJRecordInfo querySet = new TJRecordInfo(R, collection.sets[R], idx.lengths, idx.idx[R], threshold, true,
					true);

			TIntDoubleMap cands = new TIntDoubleHashMap();
			int recLength = collection.sets[R].length;
			int recMaxLength = (int) Math.floor(recLength / threshold);

			TIntSet rejected = new TIntHashSet();
			TIntSet duplicates = collection.hashGroups.get(collection.hashCodes[R]);
			if (duplicates != null) {
				rejected.addAll(duplicates);
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
					if (rejected.contains(S))
						continue;

					cands.adjustOrPutValue(S, zetUtilScore, zetUtilScore);

					double persThreshold = threshold / (1.0 + threshold) * (recLength + collection.sets[S].length);
					if (persThreshold - (cands.get(S) + querySet.sumStopped) > .0000001) {
						cands.remove(S);
						rejected.add(S);
					}
				}
			}

			TIntDoubleIterator it = cands.iterator();
			while (it.hasNext()) {
				it.advance();
				int S = it.key();
				double utilGathered = it.value();
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

				double score = eval4.verifyGraph(collection.sets[R], collection.sets[S], null,
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
		}
	}

}