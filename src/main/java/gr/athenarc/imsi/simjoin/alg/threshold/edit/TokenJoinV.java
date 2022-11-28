package gr.athenarc.imsi.simjoin.alg.threshold.edit;

import java.util.Arrays;

import org.json.simple.JSONObject;

import gnu.trove.iterator.TIntDoubleIterator;
import gnu.trove.iterator.TIntObjectIterator;
import gnu.trove.map.TIntDoubleMap;
import gnu.trove.map.hash.TIntDoubleHashMap;
import gr.athenarc.imsi.runners.ThresholdCompetitor;
import gr.athenarc.imsi.simjoin.alg.Algorithm;
import gr.athenarc.imsi.simjoin.util.GraphVerifier;
import gr.athenarc.imsi.simjoin.util.ProgressBar;
import gr.athenarc.imsi.simjoin.util.collection.Clustering;
import gr.athenarc.imsi.simjoin.util.collection.FuzzyIntSetCollection;
import gr.athenarc.imsi.simjoin.util.index.edit.FuzzySetIndex;
import gr.athenarc.imsi.simjoin.util.index.edit.IndexTokenScore;
import gr.athenarc.imsi.simjoin.util.record.RecordTokenScore;
import gr.athenarc.imsi.simjoin.util.record.edit.TJRecordInfo;

/**
 * Class for executing TokenJoin with Edit Similarity.
 *
 */
public class TokenJoinV extends Algorithm {
	long savedVer = 0, CandsGenSurvived = 0;
	String method;
	boolean self, globalOrdering, posFilter, jointFilter;
	int verificationAlg;

	public TokenJoinV(ThresholdCompetitor c) {
		super();
		this.method = c.method;
		this.self = c.self;
		this.globalOrdering = c.globalOrdering;
		this.posFilter = c.posFilter;
		this.jointFilter = c.jointFilter;
		this.verificationAlg = c.verificationAlg;
		log = new JSONObject();
		c.write(log);
	}

	/**
	 * Method to perform Join.
	 * 
	 * @param collection: Collection of records
	 * @param threshold:  Threshold of Join
	 */
	@SuppressWarnings("unchecked")
	public void selfJoin(FuzzyIntSetCollection collection, double threshold) {

		collection.clearClusterings();
		initVerificationTerms();

//		if (hybrid)
//			hybridLimit = 1 - (1 - threshold) / 2;

		System.out.println(String.format("\n%s with threshold: %s and (%s,%s,%s,%s,%s)", method, threshold, self,
				globalOrdering, posFilter, jointFilter, verificationAlg));

		joinTime = System.nanoTime();

		/* INDEX BUILDING */
		indexTime = System.nanoTime();
		FuzzySetIndex idx = new FuzzySetIndex(collection);
		indexTime = System.nanoTime() - indexTime;
		/* EXECUTE THE JOIN ALGORITHM */
		ProgressBar pb = new ProgressBar(collection.sets.length);

		TIntDoubleMap cands = new TIntDoubleHashMap();

		double uniqueToks = 0;
		for (int R = 0; R < collection.sets.length; R++) {

			// progress bar
			pb.progress(joinTime);

			/* RECORD INITIALIZATION */
			startTime = System.nanoTime();
			TJRecordInfo querySet = new TJRecordInfo(R, collection.sets[R], collection.qsets[R], idx, threshold,
					globalOrdering, self);

			signatureGenerationTime += System.nanoTime() - startTime;

			/* CANDIDATE GENERATION */
			startTime = System.nanoTime();
			int recLength = collection.sets[R].length;
			int recMaxLength = (int) Math.floor(recLength / threshold);

			int posTok = 0;
			for (posTok = 0; posTok < querySet.tokens.length; posTok++) {
				RecordTokenScore tokenScore = querySet.tokens[posTok];
				int token = tokenScore.id;
				double zetUtilScore = tokenScore.utility;

				int true_min = Arrays.binarySearch(idx.lengths[token], R); // true_min is > 0, since R is in tokenList
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

					cands.adjustOrPutValue(S, zetUtilScore, zetUtilScore);
				}
			}
			candGenands += cands.size();
			phase1Time += System.nanoTime() - startTime;

			/* CANDIDATE REFINEMENT */
			startTime = System.nanoTime();
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

				long localStartTime2 = System.nanoTime();
				refineCands++;
				double UB = 0.0;
				UB = postOpt(R, S, querySet, idx, persThreshold, utilGathered, querySet.sumStopped, posTok,
						recLength + candLength);

				jointTime += System.nanoTime() - localStartTime2;
				if (persThreshold - UB > 0.000000001)
					continue;

				verifiable++;
				/* VERIFICATION */
				Clustering[] clusteringR = collection.getClustering(R);
				Clustering[] clusteringS = collection.getClustering(S);

				GraphVerifier eval4 = new GraphVerifier();

				localStartTime2 = System.nanoTime();
				double score = eval4.verifyGraph(collection.originalStrings[R], collection.originalStrings[S], null,
						clusteringR, clusteringS, persThreshold, 0);
				long verTime1 = System.nanoTime() - localStartTime2;

				localStartTime2 = System.nanoTime();
				score = eval4.verifyGraph(collection.originalStrings[R], collection.originalStrings[S], null,
						clusteringR, clusteringS, persThreshold, 1);
				long verTime2 = System.nanoTime() - localStartTime2;

				localStartTime2 = System.nanoTime();
				score = eval4.verifyGraph(collection.originalStrings[R], collection.originalStrings[S], null,
						clusteringR, clusteringS, persThreshold, 2);
				long verTime3 = System.nanoTime() - localStartTime2;

				logger.info(String.format("bla,%s,%s,%s,%s,%s", recLength, candLength, verTime1 / 1000000000.0,
						verTime2 / 1000000000.0, verTime3 / 1000000000.0));

				verificationTime += verTime3;

				if (threshold - score > 0.000000001)
					continue;
				totalMatches++;
//				logger.info("bla," + R + "," + S + "," + score);
			}
			phase2Time += System.nanoTime() - startTime;

			uniqueToks += idx.idx[R].size();
			if ((System.nanoTime() - joinTime) / 1000000000.0 > timeOut) { // more than 5 hours
				log.put("percentage", 1.0 * R / collection.sets.length);
				break;
			}

			cands.clear();
		}

		uniqueToks /= collection.sets.length;
		joinTime = System.nanoTime() - joinTime;

		log.put("unique_tokens", uniqueToks);
		log.put("size", collection.sets.length);
		log.put("threshold", threshold);
		write_times_terms(log);
		writeVerificationTerms(log);
		logger.info(log.toString());

		System.out.println();
		System.out.println("Join time: " + joinTime / 1000000000.0 + " sec.");
		System.out.println("Number of matches: " + totalMatches);
	}

	protected double postOpt(int R, int S, TJRecordInfo querySet, FuzzySetIndex idx, double persThreshold,
			double utilGathered, double UBR, int posTok, int lengths) {

		double restUB = UBR;
		for (int t = posTok; t < querySet.tokens.length; t++) {
			RecordTokenScore tokenScore = querySet.tokens[t];
			int token = tokenScore.id;
			double zetUtilScore = tokenScore.utility;

			UBR -= zetUtilScore;
			IndexTokenScore tokS = idx.idx[S].get(token);
			restUB = UBR;
			if (tokS != null) {
				utilGathered += zetUtilScore;
				restUB = Math.min(restUB, tokS.rest2);
			}

			if (persThreshold - (restUB + utilGathered) > .0000001) {
				return (utilGathered + restUB);
			}
		}

		double total = utilGathered + UBR;
		TIntObjectIterator<IndexTokenScore> it = idx.idx[R].iterator();
		while (it.hasNext()) {
			it.advance();
			IndexTokenScore tokR = it.value();
			IndexTokenScore tokS = idx.idx[S].get(tokR.id);
			if (tokS != null) {
				total -= tokR.value;

				int len = Math.min(tokR.utilities2.length, tokS.utilities2.length) - 1;
				double utilScore = Math.min(tokR.utilities2[len], tokS.utilities2[len]);
				total += utilScore;
			}

			if (persThreshold - total > .0000001) {
				return total;
			}
		}

		return total;
	}

}