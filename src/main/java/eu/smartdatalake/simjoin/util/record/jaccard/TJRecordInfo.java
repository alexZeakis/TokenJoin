package eu.smartdatalake.simjoin.util.record.jaccard;

import java.util.Arrays;

import eu.smartdatalake.simjoin.util.index.jaccard.IndexTokenScore;
import eu.smartdatalake.simjoin.util.record.RecordTokenScore;
import gnu.trove.iterator.TIntObjectIterator;
import gnu.trove.map.TIntObjectMap;

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

	public TJRecordInfo(int index, int[][] R, int[][] lengths, TIntObjectMap<IndexTokenScore> values, double simThreshold,
			boolean globalOrdering, boolean self) {

		recordLength = R.length;

		tokens = new RecordTokenScore[values.size()];
		TIntObjectIterator<IndexTokenScore> it = values.iterator();
		int i = 0;
		while (it.hasNext()) {
			it.advance();
			double score = (globalOrdering) ? it.key() : lengths[it.key()].length / it.value().value;
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
