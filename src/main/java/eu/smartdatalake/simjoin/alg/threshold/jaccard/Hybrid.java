package eu.smartdatalake.simjoin.alg.threshold.jaccard;

import java.util.Arrays;

import org.json.simple.JSONObject;

import eu.smartdatalake.runners.ThresholdCompetitor;
import eu.smartdatalake.simjoin.alg.Algorithm;
import eu.smartdatalake.simjoin.util.GraphVerifier;
import eu.smartdatalake.simjoin.util.ProgressBar;
import eu.smartdatalake.simjoin.util.Verification;
import eu.smartdatalake.simjoin.util.collection.FuzzyIntSetCollection;
import eu.smartdatalake.simjoin.util.index.jaccard.FuzzySetIndex;
import eu.smartdatalake.simjoin.util.index.jaccard.IndexTokenScore;
import eu.smartdatalake.simjoin.util.record.RecordTokenScore;
import eu.smartdatalake.simjoin.util.record.jaccard.SMRecordInfo;
import eu.smartdatalake.simjoin.util.record.jaccard.TJRecordInfo;
import gnu.trove.iterator.TIntDoubleIterator;
import gnu.trove.map.TIntDoubleMap;
import gnu.trove.map.hash.TIntDoubleHashMap;
import gnu.trove.set.TIntSet;
import gnu.trove.set.hash.TIntHashSet;

/**
 * Class for executing Silkmoth with Jaccard Similarity.
 *
 */
public class Hybrid extends Algorithm {
	boolean self, globalOrdering;
	String method;
	int verificationAlg;

	@SuppressWarnings("unchecked")
	public Hybrid(ThresholdCompetitor c) {
		this.method = c.method;
		this.self = c.self;
		this.globalOrdering = c.globalOrdering;
		this.verificationAlg = c.verificationAlg;
		log = new JSONObject();
		JSONObject args = new JSONObject();
		args.put("self", self);
		log.put("args", args);
	}

	/**
	 * Method to perform Join.
	 * @param collection: Collection of records
	 * @param threshold: Threshold of Join
	 */
	@SuppressWarnings("unchecked")
	public void selfJoin(FuzzyIntSetCollection collection, double threshold) {

		collection.clearClusterings();
		initVerificationTerms();

		System.out.println(String.format("\nSilkmoth with threshold: %s", threshold));

		joinTime = System.nanoTime();

		indexTime = System.nanoTime();
		FuzzySetIndex idx = new FuzzySetIndex();
		idx.buildIndex(collection);
		indexTime = System.nanoTime() - indexTime;

		/* EXECUTE THE JOIN ALGORITHM */
		ProgressBar pb = new ProgressBar(collection.sets.length);

		for (int R = 0; R < collection.sets.length; R++) {
			// progress bar
			pb.progress(joinTime);

			/* SIGNATURE GENERATION */
			long localStartTime = System.nanoTime();
			SMRecordInfo querySet_SM = new SMRecordInfo(R, collection.sets[R], idx.lengths, idx.idx[R], globalOrdering);
			querySet_SM.computeUnflattenedSignature(idx, threshold, self, collection.sets[R]);
			TJRecordInfo querySet_TJ = new TJRecordInfo(R, collection.sets[R], idx.lengths, idx.idx[R], threshold,
					globalOrdering, self);
			signatureGenerationTime += System.nanoTime() - localStartTime;

			localStartTime = System.nanoTime();

			/* CANDIDATE GENERATION */
			TIntDoubleMap cands = new TIntDoubleHashMap();
			int recLength = collection.sets[R].length;
			int recMaxLength = (int) Math.floor(recLength / threshold);

			int posTok = 0;
			for (posTok = 0; posTok < querySet_TJ.tokens.length; posTok++) {
				RecordTokenScore tokenScore = querySet_TJ.tokens[posTok];
				int token = tokenScore.id;
				double zetUtilScore = tokenScore.utility;

				int true_min = Arrays.binarySearch(idx.lengths[token], R); // true_min is > 0, since R is in tokenList
				int[] tempCands = idx.lengths[token];
				int tempLen = tempCands.length;

				if (querySet_TJ.theta - querySet_TJ.sumStopped > 0.0000001)
					break;

				querySet_TJ.sumStopped -= zetUtilScore;

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
			candGenTime += System.nanoTime() - localStartTime;

			TIntDoubleIterator it2 = cands.iterator();
			while (it2.hasNext()) {
				it2.advance();
				int S = it2.key();
				int candLength = collection.sets[S].length;
				long localStartTime2 = System.nanoTime();
				double[][] hits = new double[recLength][];
				double[] nearestNeighborSim = new double[recLength];
				initTime += System.nanoTime() - localStartTime2;

				localStartTime2 = System.nanoTime();
				/* CHECK FILTER */

				boolean pass = false;
				for (int r = 0; r < querySet_SM.unflattenedSignature.length; r++) {
					hits[r] = new double[candLength];
					double maxNN = 0.0;

					for (int t = 0; t < querySet_SM.unflattenedSignature[r].size(); t++) {
						int token = querySet_SM.unflattenedSignature[r].get(t);
						IndexTokenScore tok = idx.idx[S].get(token);
						if (tok == null)
							continue;
						for (int s : tok.elements) {
							hits[r][s] = Verification.verifyWithScore(collection.sets[R][r], collection.sets[S][s]);
							if (hits[r][s] >= querySet_SM.elementBounds[r]) {
								pass = true;
							}
							maxNN = Math.max(hits[r][s], maxNN);
						}
					}

					if (maxNN > 0) { // element had signature tokens
						nearestNeighborSim[r] = maxNN;
					}

				}

				CFTime += System.nanoTime() - localStartTime2;
				if (!pass) {
					continue;
				}
				CFCands++;

				/* NEAREST NEIGHBOR FILTER */
				localStartTime2 = System.nanoTime();
				double persThreshold = threshold / (1.0 + threshold) * (recLength + candLength);
				TIntSet matchedElements;

				double totalUB = 0;
				matchedElements = new TIntHashSet();
				for (int r = 0; r < collection.sets[R].length; r++) {
					if (nearestNeighborSim[r] > querySet_SM.elementBounds[r]) {
						matchedElements.add(r);
						totalUB += nearestNeighborSim[r];
					} else {
						totalUB += querySet_SM.elementBounds[r];
					}
				}

				for (int r = 0; r < recLength; r++) {
					if (matchedElements.contains(r)) {
						continue;
					}

					totalUB -= querySet_SM.elementBounds[r];

					double maxSim = 0.0;
					double UBStep = 1.0 / collection.sets[R][r].length;
					double elemUB = 1.0;
					for (int token : collection.sets[R][r]) {
						elemUB -= UBStep;

						IndexTokenScore tok = idx.idx[S].get(token);
						if (tok == null)
							continue;
						if (tok != null) {
							if (hits[r] == null)
								hits[r] = new double[candLength];
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
					nearestNeighborSim[r] = maxSim;

					totalUB += maxSim;

					if (persThreshold - totalUB > 0.000000001) {
						break;
					}
				}

				double score = totalUB / (recLength + candLength - totalUB);
				NNFTime += System.nanoTime() - localStartTime2;
				if (threshold - score > 0.000000001) {
					continue;
				}

				NNFCands++;

				verifiable++;
				/* VERIFICATION */
				localStartTime2 = System.nanoTime();
				GraphVerifier eval4 = new GraphVerifier();
				score = eval4.verifyGraph(collection.sets[R], collection.sets[S], hits, collection.getClustering(R),
						collection.getClustering(S), persThreshold, verificationAlg);

				verificationTime += System.nanoTime() - localStartTime2;

				if (threshold - score > 0.000000001)
					continue;
				totalMatches++;
//				System.out.println(cm);
			}
			
			if ((System.nanoTime() - joinTime) / 1000000000.0 > timeOut) { // more than 5 hours
				log.put("percentage", 1.0 * R / collection.sets.length);
				break;
			}

		}
		joinTime = System.nanoTime() - joinTime;

		log.put("name", method);
		log.put("size", collection.sets.length);
		log.put("threshold", threshold);
		writeSilkmothStats(log);
		writeVerificationTerms(log);
		logger.info(log.toString());

		System.out.println();
		System.out.println("Join time: " + joinTime / 1000000000.0 + " sec.");
		System.out.println("Number of matches: " + totalMatches);

	}
}