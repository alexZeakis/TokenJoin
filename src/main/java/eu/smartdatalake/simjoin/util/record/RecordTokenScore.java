package eu.smartdatalake.simjoin.util.record;

/**
 * Wrapper class for a token, containing its id, utility and score (cost or
 * cost/value). Used for ordering tokens for a records
 *
 */
public class RecordTokenScore implements Comparable<RecordTokenScore> {
	public int id;
	public double score, utility;

	public RecordTokenScore(int id, double score, double utility) {
		this.id = id;
		this.score = score;
		this.utility = utility;
	}

	@Override
	public int compareTo(RecordTokenScore o) {
		return Double.compare(this.score, o.score); // ascending order
	}

	@Override
	public String toString() {
		return String.format("(%s,%.2f)", id, utility);
	}
}