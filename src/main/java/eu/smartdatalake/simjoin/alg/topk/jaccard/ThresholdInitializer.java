package eu.smartdatalake.simjoin.alg.topk.jaccard;

import java.util.Arrays;
import java.util.Collections;
import java.util.PriorityQueue;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.collect.MinMaxPriorityQueue;

import eu.smartdatalake.simjoin.alg.topk.CandPair;
import eu.smartdatalake.simjoin.alg.topk.KPair;
import eu.smartdatalake.simjoin.util.GraphVerifier;
import eu.smartdatalake.simjoin.util.ProgressBar;
import eu.smartdatalake.simjoin.util.collection.FuzzyIntSetCollection;
import eu.smartdatalake.simjoin.util.collection.FuzzySetCollectionReader;
import eu.smartdatalake.simjoin.util.index.jaccard.FuzzySetIndex;
import eu.smartdatalake.simjoin.util.index.jaccard.IndexTokenScore;
import eu.smartdatalake.simjoin.util.record.RecordTokenScore;
import eu.smartdatalake.simjoin.util.record.jaccard.TJRecordInfo;
import gnu.trove.iterator.TIntDoubleIterator;
import gnu.trove.map.TIntDoubleMap;
import gnu.trove.map.hash.TIntDoubleHashMap;
import gnu.trove.set.TIntSet;
import gnu.trove.set.hash.TIntHashSet;

class ThresholdInitializer {
	protected static final Logger logger = LogManager.getLogger(FuzzySetCollectionReader.class);

	double samplePercentage;
	double deltaGeneration, mu;
	int lambda;

	public ThresholdInitializer(double deltaGeneration, double mu, int lambda, double samplePercentage) {
		this.deltaGeneration = deltaGeneration;
		this.mu = mu;
		this.lambda = lambda;
		this.samplePercentage = samplePercentage;
	}

	double initThreshold(int choice, int k, FuzzyIntSetCollection collection, FuzzySetIndex idx, PriorityQueue<KPair> B,
			TIntSet[] cRejected, long joinTime) {

		double threshold = 0.0;
		if (choice == 0) {

		} else if (choice == 1) {
			int i = 0;
			while (B.size() != k) {
				int limit = Math.min(i + k, collection.sets.length);
				for (int j = i + 1; j < limit; j++) {

					/* VERIFICATION */
					GraphVerifier eval4 = new GraphVerifier();
					double score = eval4.verifyGraph(collection.sets[i], collection.sets[j], null,
							collection.getClustering(i), collection.getClustering(j), 0, 1);

					if (cRejected[i] == null)
						cRejected[i] = new TIntHashSet();
					cRejected[i].add(j);

					if (score == 1.0 || score == 0.0)
						continue;

					KPair pp = new KPair(i, j, score);
					B.add(pp);
					if (B.size() > k) {
						B.poll();
					}
					if (B.size() == k & B.peek().score > 0)
						return B.peek().score;
				}
				i++;
				if (i > collection.sets.length)
					break;
			}
		} else if (choice == 2) {

			int totalVerifications = lambda * k;

			MinMaxPriorityQueue<CandPair> GQ3 = MinMaxPriorityQueue.orderedBy(Collections.reverseOrder())
					.maximumSize(totalVerifications).create();

			int sampleSize = (int) (collection.sets.length * samplePercentage);
			ProgressBar pb = new ProgressBar(sampleSize);
			
			int mu = (int) (this.mu * k);
			
			TIntDoubleMap cands = new TIntDoubleHashMap();
			
			for (int R = 0; R < sampleSize; R++) {

				// progress bar
				pb.progress(joinTime);

				TJRecordInfo querySet = new TJRecordInfo(R, collection.sets[R], idx.lengths, idx.idx[R],
						deltaGeneration, true, true);

				int recLength = collection.sets[R].length;
				int recMaxLength = (int) Math.floor(recLength / deltaGeneration);

				TIntSet localRejected = new TIntHashSet();
				TIntSet duplicates = collection.hashGroups.get(collection.hashCodes[R]);
				if (duplicates != null) {
					localRejected.addAll(duplicates);
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

				TIntDoubleIterator cit = cands.iterator();
				PriorityQueue<CandPair> Q = new PriorityQueue<CandPair>(Collections.reverseOrder());
				while (cit.hasNext()) {
					cit.advance();
					int S = cit.key();
					double total = cit.value() + querySet.sumStopped;
					double score = total / (collection.sets[R].length + collection.sets[S].length - total);
					Q.add(new CandPair(R, S, score, cit.value()));
				}

				int candsExamined = 0;
				while (!Q.isEmpty()) {
					CandPair p = Q.poll();
					int S = p.right;
					double utilGathered = p.utilGathered;
					double sumStopped = querySet.sumStopped;

					for (int localPosTok = posTok; localPosTok < querySet.tokens.length; localPosTok++) {
						RecordTokenScore tokenScore = querySet.tokens[localPosTok];
						double zetUtilScore = tokenScore.utility;

						sumStopped -= zetUtilScore;

						IndexTokenScore tokS = idx.idx[S].get(tokenScore.id);
						if (tokS != null) {
							utilGathered += zetUtilScore;
						}
					}

					double total = sumStopped + utilGathered;
					p.score = total / (collection.sets[R].length + collection.sets[S].length - total);
					GQ3.add(p);

					candsExamined++;
					if (candsExamined >= mu)
						break;
				}
				cands.clear();
			}


			int verified = 0;
			while (!GQ3.isEmpty() && GQ3.peekFirst().score > threshold) {
				CandPair p = GQ3.pollFirst();
			
				int R = p.left;
				int S = p.right;

				if (verified % 1000 == 0 & B.size() > 0)
					System.out.println(verified + "\t" + B.size() + " " + B.peek().score);

				if (cRejected[R] == null)
					cRejected[R] = new TIntHashSet();
				cRejected[R].add(S); // remove for not re-evaluating later

				/* VERIFICATION */
				GraphVerifier eval4 = new GraphVerifier();

				// Calling VUB -> 1
				double score = eval4.verifyGraph(collection.sets[R], collection.sets[S], null,
						collection.getClustering(R), collection.getClustering(S), 0, 1);

				if (score == 1.0)
					continue;

				KPair pp = new KPair(R, S, score);
				B.add(pp);
				if (B.size() > k) {
					B.poll();
					threshold = B.peek().score;
				}

				verified++;
			}
		}
		return threshold;
	}
}
