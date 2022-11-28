package gr.athenarc.imsi.simjoin.util.record.edit;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import gnu.trove.iterator.TIntObjectIterator;
import gnu.trove.list.TIntList;
import gnu.trove.list.array.TIntArrayList;
import gr.athenarc.imsi.simjoin.util.index.edit.FuzzySetIndex2;
import gr.athenarc.imsi.simjoin.util.index.edit.IndexTokenScore2;
import gr.athenarc.imsi.simjoin.util.record.RecordTokenScore;

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

	public SMRecordInfo(int index, int[][] querySet, int[][] qR, FuzzySetIndex2 idx,
			boolean globalOrdering) {
		this.index = index;
		recordLength = querySet.length;

		List<RecordTokenScore> tempTokens = new ArrayList<RecordTokenScore>();
		TIntObjectIterator<IndexTokenScore2> it = idx.idx[index].iterator();
		while (it.hasNext()) {
			it.advance();
			double value = it.value().value;
			if (value == 0.0) // not a qchunk
				continue;
			double cost = idx.recordIndex[it.key()].length;
			double score = (globalOrdering) ? it.key() : cost / value;
			tempTokens.add(new RecordTokenScore(it.key(), score, value));
		}
		tokens = new RecordTokenScore[tempTokens.size()];
		for (int i = 0; i < tempTokens.size(); i++) {
			tokens[i] = tempTokens.get(i);
		}
		Arrays.parallelSort(tokens);
	}
	
	public void computeUnflattenedSignature(FuzzySetIndex2 idx, double simThreshold, boolean self, String[] strings) {
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

//			// update the signature
			for (int r : idx.idx[index].get(bestToken).elements)
				unflattenedSignature[r].add(bestToken);
		}

		elementBounds = new double[strings.length];
		for (int r = 0; r < strings.length; r++) {
			elementBounds[r] = 1 - 1.0 * unflattenedSignature[r].size() / strings[r].length();
		}
	}
}