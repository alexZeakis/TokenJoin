package eu.smartdatalake.simjoin.alg.threshold.edit;

import java.util.Arrays;

import org.json.simple.JSONObject;

import eu.smartdatalake.runners.ThresholdCompetitor;
import eu.smartdatalake.simjoin.alg.Algorithm;
import eu.smartdatalake.simjoin.util.GraphVerifier;
import eu.smartdatalake.simjoin.util.ProgressBar;
import eu.smartdatalake.simjoin.util.Verification;
import eu.smartdatalake.simjoin.util.collection.FuzzyIntSetCollection;
import eu.smartdatalake.simjoin.util.index.edit.FuzzySetIndex2;
import eu.smartdatalake.simjoin.util.index.edit.IndexTokenScore2;
import eu.smartdatalake.simjoin.util.record.edit.SMRecordInfo;
import gnu.trove.iterator.TIntDoubleIterator;
import gnu.trove.iterator.TIntIterator;
import gnu.trove.map.TIntDoubleMap;
import gnu.trove.map.hash.TIntDoubleHashMap;
import gnu.trove.set.TIntSet;
import gnu.trove.set.hash.TIntHashSet;

/**
 * Class for executing Silkmoth with Edit Similarity.
 *
 */
public class Silkmoth extends Algorithm {
	boolean self, globalOrdering;
	String method;
	int verificationAlg;

	@SuppressWarnings("unchecked")
	public Silkmoth(ThresholdCompetitor c) {
		this.method = c.method;
		this.self = c.self;
		this.globalOrdering = c.globalOrdering;
		this.verificationAlg = c.verificationAlg;
		log = new JSONObject();
		JSONObject args = new JSONObject();
		args.put("self", self);
		args.put("verificationAlg", verificationAlg);
		log.put("args", args);
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

		System.out.println(String.format("\nEditSilkmoth with threshold: %s", threshold));

		joinTime = System.nanoTime();

		/* BUILDING INDEX */
		long indexTime = System.nanoTime();
		FuzzySetIndex2 idx = new FuzzySetIndex2(collection);
		indexTime = System.nanoTime() - indexTime;

		int maxRecLength = collection.sets[collection.sets.length - 1].length;
		double[][] hits = new double[maxRecLength][];
		for (int nor = 0; nor < maxRecLength; nor++)
			hits[nor] = new double[maxRecLength];
		boolean[] matchedElements = new boolean[maxRecLength];
		TIntSet cands = new TIntHashSet();
		TIntDoubleMap[] candsElements = new TIntDoubleMap[collection.sets.length];
		for (int S = 0; S < collection.sets.length; S++)
			candsElements[S] = new TIntDoubleHashMap();

		/* EXECUTE THE JOIN ALGORITHM */
		ProgressBar pb = new ProgressBar(collection.sets.length);

		for (int R = 0; R < collection.sets.length; R++) {

			// progress bar
			pb.progress(joinTime);

			int[][] RR = collection.sets[R];
			int[][] QR = collection.qsets[R];
			String[] RRR = collection.originalStrings[R];

			/* SIGNATURE GENERATION */
			long localStartTime = System.nanoTime();
			SMRecordInfo querySet = new SMRecordInfo(R, RR, QR, idx, globalOrdering);
			querySet.computeUnflattenedSignature(idx, threshold, self, RRR);
			signatureGenerationTime += System.nanoTime() - localStartTime;

			localStartTime = System.nanoTime();

			// compute bounds for length filter
			int recLength = collection.sets[R].length;
			int recMaxLength = (int) Math.floor(recLength / threshold);

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
						int s = elementCands[Si];
						if (R == S)
							continue;
						if (collection.sets[S].length > recMaxLength) {
							break;
						}

						double score = Verification.verifyWithScore(collection.originalStrings[R][r],
								collection.originalStrings[S][s]);
						if (score >= querySet.elementBounds[r]) {
							cands.add(S);
							double val = candsElements[S].get(r);
							if (val == candsElements[S].getNoEntryValue() || score > val)
								candsElements[S].put(r, score);
						}
					}
				}
			}

			CFCands += cands.size();
			candGenTime += System.nanoTime() - localStartTime;


			TIntIterator it2 = cands.iterator();
			while (it2.hasNext()) {
				int S = it2.next();

				long localStartTime2 = System.nanoTime();
				int candLength = collection.sets[S].length;
				initTime += System.nanoTime() - localStartTime2;

				/* NEAREST NEIGHBOR FILTER */
				localStartTime2 = System.nanoTime();
				double persThreshold = threshold / (1.0 + threshold) * (recLength + candLength);
				
				// INIT TOTALUB
				double totalUB = 0;
				for (int r = 0; r < collection.sets[R].length; r++) {
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
				

				for (int r = 0; r < recLength; r++) {
					if (matchedElements[r]) {
						continue;
					}
					for (int nos = 0; nos < candLength; nos++)
						hits[r][nos] = 0.0;

					totalUB -= querySet.elementBounds[r];

					int q = 3;
					double maxSim = (q - 1.0) / q;

					double elemUB = 1.0;
					for (int ti = 0; ti < collection.qsets[R][r].length; ti++) {
						elemUB = 1.0 * collection.originalStrings[R][r].length()
								/ (collection.originalStrings[R][r].length() + ti + 1);
						int token = collection.qsets[R][r][ti];

						IndexTokenScore2 tok = idx.idx[S].get(token);
						if (tok == null)
							continue;
						if (tok != null) {
							for (int s : tok.elements) {
								if (hits[r][s] == 0.0) {
									hits[r][s] = Verification.verifyWithScore(collection.originalStrings[R][r],
											collection.originalStrings[S][s]);
								}
								maxSim = Math.max(maxSim, hits[r][s]);

							}
						}
						if (maxSim - elemUB >= 0.00000001)
							break;
					}
					totalUB += Math.max(elemUB, maxSim);

					if (persThreshold - totalUB > 0.000000001) {
						break;
					}
				}
				NNFTime += System.nanoTime() - localStartTime2;
				double score = totalUB / (recLength + candLength - totalUB);

				if (threshold - score > 0.000000001) {
					continue;
				}
				NNFCands++;

				verifiable++;
				/* VERIFICATION */
				localStartTime2 = System.nanoTime();
				GraphVerifier eval4 = new GraphVerifier();
				score = eval4.verifyGraph(collection.originalStrings[R], collection.originalStrings[S], hits,
						collection.getClustering(R), collection.getClustering(S), persThreshold, verificationAlg);
				verificationTime += System.nanoTime() - localStartTime2;

				if (threshold - score > 0.000000001)
					continue;
				totalMatches++;
			}

			if ((System.nanoTime() - joinTime) / 1000000000.0 > timeOut) { // more than 5 hours
				log.put("percentage", 1.0 * R / collection.sets.length);
				break;
			}

			cands.clear();
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
//		System.out.println(candGenands);
	}
}