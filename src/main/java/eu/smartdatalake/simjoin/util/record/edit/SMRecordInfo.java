package eu.smartdatalake.simjoin.util.record.edit;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import eu.smartdatalake.simjoin.util.index.edit.FuzzySetIndex;
import eu.smartdatalake.simjoin.util.index.edit.IndexTokenScore;
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

	public SMRecordInfo(int index, int[][] querySet, int[][] qR, int[][] lengths, TIntObjectMap<IndexTokenScore> values,
			boolean globalOrdering) {
		this.index = index;
		recordLength = querySet.length;

		List<RecordTokenScore> tempTokens = new ArrayList<RecordTokenScore>();
		TIntObjectIterator<IndexTokenScore> it = values.iterator();
		while (it.hasNext()) {
			it.advance();
			if (it.value().value == 0.0) // not a qchunk
				continue;
			double score = (globalOrdering) ? it.key(): lengths[it.key()].length / it.value().value;
			tempTokens.add(new RecordTokenScore(it.key(), score, it.value().value));
		}
		tokens = new RecordTokenScore[tempTokens.size()];
		for (int i = 0; i < tempTokens.size(); i++) {
			tokens[i] = tempTokens.get(i);
		}
		Arrays.parallelSort(tokens);
	}

	public void computeUnflattenedSignature(FuzzySetIndex idx, double simThreshold, boolean self, String[] strings) {
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

		elementBounds = new double[strings.length];
		for (int r = 0; r < strings.length; r++) {
			elementBounds[r] = 1 - 1.0 * unflattenedSignature[r].size() / strings[r].length();
		}
	}
}