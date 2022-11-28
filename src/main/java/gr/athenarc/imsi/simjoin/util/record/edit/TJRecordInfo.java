package gr.athenarc.imsi.simjoin.util.record.edit;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import gnu.trove.iterator.TIntObjectIterator;
import gr.athenarc.imsi.simjoin.util.index.edit.FuzzySetIndex;
import gr.athenarc.imsi.simjoin.util.index.edit.IndexTokenScore;
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

	public TJRecordInfo(int index, int[][] R, int[][] qR, FuzzySetIndex idx,
			double simThreshold, boolean globalOrdering, boolean self) {


		recordLength = R.length;

		List<RecordTokenScore> tempTokens = new ArrayList<RecordTokenScore>(); 
		TIntObjectIterator<IndexTokenScore> it = idx.idx[index].iterator();
		while (it.hasNext()) {
			it.advance();
			if (it.value().value == 0.0)	//not a qchunk
				continue;
			double score = (globalOrdering) ? it.key() : idx.lengths[it.key()].length / it.value().value;
			tempTokens.add(new RecordTokenScore(it.key(), score, it.value().value));
		}
		tokens = new RecordTokenScore[tempTokens.size()];
		for (int i=0; i<tempTokens.size(); i++) {
			tokens[i] = tempTokens.get(i);
		}
		Arrays.parallelSort(tokens);
		
		if (self)
			theta = 2 * simThreshold / (1 + simThreshold) * recordLength;
		else
			theta = simThreshold * recordLength;
		sumStopped = recordLength;
	}
	
	public void changeTheta(double simThreshold) {
		if (self)
			theta = 2 * simThreshold / (1 + simThreshold) * recordLength;
		else
			theta = simThreshold * recordLength;
	}
}
