package gr.athenarc.imsi.simjoin.util.record.jaccard;

import java.util.Arrays;

import gnu.trove.iterator.TIntObjectIterator;
import gr.athenarc.imsi.simjoin.util.index.jaccard.FuzzySetIndex;
import gr.athenarc.imsi.simjoin.util.index.jaccard.IndexTokenScore;
import gr.athenarc.imsi.simjoin.util.record.RecordTokenScore;

/**
 * Class containing necessary information about a record, i.e. order of tokens and θ.
 *
 */
public class TJRecordInfo{
	boolean self;
	
	public double sumStopped;

	public int recordLength;
	public double theta;
	public RecordTokenScore[] tokens;

	public TJRecordInfo(int index, int[][] R, FuzzySetIndex idx, double simThreshold,
			boolean globalOrdering, boolean self) {

		recordLength = R.length;

		tokens = new RecordTokenScore[idx.idx[index].size()];
		TIntObjectIterator<IndexTokenScore> it = idx.idx[index].iterator();
		int i = 0;
		while (it.hasNext()) {
			it.advance();
			double score = (globalOrdering) ? it.key() : idx.lengths[it.key()].length / it.value().value;
			tokens[i++] = new RecordTokenScore(it.key(), score, it.value().value);
		}
		Arrays.parallelSort(tokens);

		if (self)
			theta = 2 * simThreshold / (1 + simThreshold) * recordLength;
		else
			theta = simThreshold * recordLength;
		sumStopped = recordLength;
		
		this.self = self;
	}

	public void changeTheta(double simThreshold) {
		if (self)
			theta = 2 * simThreshold / (1 + simThreshold) * recordLength;
		else
			theta = simThreshold * recordLength;
	}
}
