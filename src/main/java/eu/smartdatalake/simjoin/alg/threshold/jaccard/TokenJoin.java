package eu.smartdatalake.simjoin.alg.threshold.jaccard;

import java.util.Arrays;

import org.json.simple.JSONObject;

import eu.smartdatalake.runners.ThresholdCompetitor;
import eu.smartdatalake.simjoin.alg.Algorithm;
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

/**
 * Class for executing TokenJoin with Jaccard Similarity.
 *
 */
public class TokenJoin extends Algorithm {
	String method;
	boolean self, globalOrdering, posFilter, jointFilter;
	int verificationAlg;

	public TokenJoin(ThresholdCompetitor c) {
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
		FuzzySetIndex idx = new FuzzySetIndex();
		idx.buildIndex(collection);
		indexTime = System.nanoTime() - indexTime;
		/* EXECUTE THE JOIN ALGORITHM */
		ProgressBar pb = new ProgressBar(collection.sets.length);

		double uniqueToks = 0;
		for (int R = 0; R < collection.sets.length; R++) {

			// progress bar
			pb.progress(joinTime);

			/* RECORD INITIALIZATION */
			startTime = System.nanoTime();
			TJRecordInfo querySet = new TJRecordInfo(R, collection.sets[R], idx.lengths, idx.idx[R], threshold,
					globalOrdering, self);

			signatureGenerationTime += System.nanoTime() - startTime;

			/* CANDIDATE GENERATION */
			startTime = System.nanoTime();
			TIntDoubleMap cands = new TIntDoubleHashMap();
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

				if (jointFilter)
					UB = postJoint(R, S, querySet, idx, persThreshold, utilGathered, querySet.sumStopped, posTok);
				else if (posFilter)
					UB = postPositional(R, S, querySet, idx, persThreshold, utilGathered, querySet.sumStopped, posTok);
				else
					UB = postBasic(R, S, querySet, idx, persThreshold, total, posTok);
				
				jointTime += System.nanoTime() - localStartTime2;
				if (persThreshold - UB > 0.000000001)
					continue;

				verifiable++;
				/* VERIFICATION */
				localStartTime2 = System.nanoTime();
				GraphVerifier eval4 = new GraphVerifier();

				double score = eval4.verifyGraph(collection.sets[R], collection.sets[S], null,
						collection.getClustering(R), collection.getClustering(S), persThreshold, verificationAlg);

				verificationTime += System.nanoTime() - localStartTime2;

				if (threshold - score > 0.000000001)
					continue;
				totalMatches++;
			}
			phase2Time += System.nanoTime() - startTime;

			uniqueToks += idx.idx[R].size();
			if ((System.nanoTime() - joinTime) / 1000000000.0 > timeOut) { // more than 5 hours
				log.put("percentage", 1.0 * R / collection.sets.length);
				break;
			}
		}

		uniqueToks /= collection.sets.length;
		joinTime = System.nanoTime() - joinTime;

		log.put("size", collection.sets.length);
		log.put("unique_tokens", uniqueToks);
		log.put("threshold", threshold);
		write_times_terms(log);
		writeVerificationTerms(log);
		logger.info(log.toString());

		System.out.println();
		System.out.println("Join time: " + joinTime / 1000000000.0 + " sec.");
		System.out.println("Number of matches: " + totalMatches);
	}

	protected double postJoint(int R, int S, TJRecordInfo querySet, FuzzySetIndex idx, double persThreshold,
			double utilGathered, double UBR, int posTok) {
	
		for (int t = posTok; t < querySet.tokens.length; t++) {
			RecordTokenScore tokenScore = querySet.tokens[t];
			int token = tokenScore.id;
			double zetUtilScore = tokenScore.utility;
	
			UBR -= tokenScore.utility;
	
			IndexTokenScore tokS = idx.idx[S].get(token);
			if (tokS != null) {
				utilGathered += zetUtilScore;

				if (persThreshold - (UBR + utilGathered) > .0000001) {
					return (utilGathered + UBR);
				}

				if (persThreshold - (tokS.rest + utilGathered) > .0000001) {
					return (utilGathered + tokS.rest);
				}
			} else {
				if (persThreshold - (UBR + utilGathered) > .0000001) {
					return (utilGathered + UBR);
				}
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
	
				int len = Math.min(tokR.utilities.length, tokS.utilities.length) - 1;
				double utilScore = Math.min(tokR.utilities[len], tokS.utilities[len]);
				total += utilScore;
			}
	
			if (persThreshold - total > .0000001) {
				return total;
			}
		}
	
		return total;
	}

	protected double postPositional(int R, int S, TJRecordInfo querySet, FuzzySetIndex idx, double persThreshold,
			double utilGathered, double UBR, int posTok) {

		for (int t = posTok; t < querySet.tokens.length; t++) {
			RecordTokenScore tokenScore = querySet.tokens[t];
			int token = tokenScore.id;
			double zetUtilScore = tokenScore.utility;

			UBR -= tokenScore.utility;

			IndexTokenScore tokS = idx.idx[S].get(token);
			if (tokS != null) {
				utilGathered += zetUtilScore;

				if (persThreshold - (UBR + utilGathered) > .0000001) {
					return (utilGathered + UBR);
				}

				if (persThreshold - (tokS.rest + utilGathered) > .0000001) {
					return (utilGathered + tokS.rest);
				}
			} else {
				if (persThreshold - (UBR + utilGathered) > .0000001) {
					return (utilGathered + UBR);
				}
			}
		}

		return utilGathered + UBR;
	}

	protected double postBasic(int R, int S, TJRecordInfo querySet, FuzzySetIndex idx, double persThreshold,
			double total, int posTok) {

		for (int t = posTok; t < querySet.tokens.length; t++) {
			RecordTokenScore tokenScore = querySet.tokens[t];
			int token = tokenScore.id;

			IndexTokenScore tokS = idx.idx[S].get(token);
			if (tokS == null)
				total -= tokenScore.utility;

			if (persThreshold - total > .0000001)
				return total;
		}
		return total;
	}
}