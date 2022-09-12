package eu.smartdatalake.simjoin.util.record.jaccard;

import java.util.Arrays;

import eu.smartdatalake.simjoin.util.index.jaccard.FuzzySetIndex;
import eu.smartdatalake.simjoin.util.index.jaccard.FuzzySetIndex2;
import eu.smartdatalake.simjoin.util.index.jaccard.IndexTokenScore;
import eu.smartdatalake.simjoin.util.index.jaccard.IndexTokenScore2;
import eu.smartdatalake.simjoin.util.record.RecordTokenScore;
import gnu.trove.iterator.TIntObjectIterator;
import gnu.trove.list.TIntList;
import gnu.trove.list.array.TIntArrayList;
import gnu.trove.map.TIntObjectMap;

/**
 * Class containing necessary information about a record, i.e. order of tokens,
 * θ and signatures.
 *
 */
public class SMRecordInfo {
	public static long cachedCounter = 0, counter = 0, cachedSimilar = 0;

	public int index;
	public int recordLength;
	public double theta;
	public RecordTokenScore[] tokens;

	public TIntList[] unflattenedSignature;
	public double[] elementBounds;
	public double simUpperBound;

	public TIntList KTR;

	public SMRecordInfo(int index, int[][] querySet, int[][] lengths, TIntObjectMap<IndexTokenScore> values,
			boolean globalOrdering) {
		this.index = index;
		recordLength = querySet.length;

		// Compute token scores
		tokens = new RecordTokenScore[values.size()];
		TIntObjectIterator<IndexTokenScore> it = values.iterator();
		int i = 0;
		while (it.hasNext()) {
			it.advance();
			double score = (globalOrdering) ? it.key() : lengths[it.key()].length / it.value().value;
			tokens[i++] = new RecordTokenScore(it.key(), score, it.value().value);
		}

		Arrays.parallelSort(tokens);
	}

	public void computeUnflattenedSignature(FuzzySetIndex idx, double simThreshold, boolean self, int[][] R) {
		KTR = new TIntArrayList();

		if (self)
			this.theta = 2 * simThreshold / (1 + simThreshold) * recordLength;
		else
			this.theta = simThreshold * recordLength;
		simUpperBound = recordLength;

		unflattenedSignature = new TIntList[recordLength];
		for (int r = 0; r < recordLength; r++) {
			unflattenedSignature[r] = new TIntArrayList();
		}

		// construct the signature
		for (int curToken = 0; curToken < tokens.length; curToken++) {
			if (this.theta - simUpperBound > 0.0000001) {
				break;
			}
			simUpperBound -= tokens[curToken].utility;
			int bestToken = tokens[curToken].id;
			KTR.add(bestToken);

//			// update the signature
			for (int r : idx.idx[index].get(bestToken).elements)
				unflattenedSignature[r].add(bestToken);
		}

		elementBounds = new double[R.length];
		for (int r = 0; r < R.length; r++) {
			elementBounds[r] = (double) (R[r].length - unflattenedSignature[r].size()) / (double) R[r].length;

		}
	}
	
	public SMRecordInfo(int index, int[][] querySet, int[] costs, TIntObjectMap<IndexTokenScore2> values,
			boolean globalOrdering) {
		this.index = index;
		recordLength = querySet.length;

		// Compute token scores
		tokens = new RecordTokenScore[values.size()];
		TIntObjectIterator<IndexTokenScore2> it = values.iterator();
		int i = 0;
		while (it.hasNext()) {
			it.advance();
			double score = (globalOrdering) ? it.key() : costs[it.key()] / it.value().value;
			tokens[i++] = new RecordTokenScore(it.key(), score, it.value().value);
		}

		Arrays.parallelSort(tokens);
	}
	
	public void computeUnflattenedSignature(FuzzySetIndex2 idx, double simThreshold, boolean self, int[][] R) {
		KTR = new TIntArrayList();

		if (self)
			this.theta = 2 * simThreshold / (1 + simThreshold) * recordLength;
		else
			this.theta = simThreshold * recordLength;
		simUpperBound = recordLength;

		unflattenedSignature = new TIntList[recordLength];
		for (int r = 0; r < recordLength; r++) {
			unflattenedSignature[r] = new TIntArrayList();
		}

		// construct the signature
		for (int curToken = 0; curToken < tokens.length; curToken++) {
			if (this.theta - simUpperBound > 0.0000001) {
				break;
			}
			simUpperBound -= tokens[curToken].utility;
			int bestToken = tokens[curToken].id;
			KTR.add(bestToken);

//			// update the signature
			for (int r : idx.idx[index].get(bestToken).elements)
				unflattenedSignature[r].add(bestToken);
		}

		elementBounds = new double[R.length];
		for (int r = 0; r < R.length; r++) {
			elementBounds[r] = (double) (R[r].length - unflattenedSignature[r].size()) / (double) R[r].length;

		}
	}
}