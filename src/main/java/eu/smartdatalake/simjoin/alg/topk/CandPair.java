package eu.smartdatalake.simjoin.alg.topk;


/**
 * Class for storing Top-K pairs
 *
 */
public class CandPair implements Comparable<CandPair> {
	public int left, right;
	public double score, utilGathered;

	public CandPair(int left, int right, double score, double utilGathered) {
		this.left = left;
		this.right = right;
		this.score = score;
		this.utilGathered = utilGathered;
	}
	
	@Override
	public int compareTo(CandPair o) {
		if (this.score == o.score) {
			if (this.left == o.left)
				return Integer.compare(this.right, o.right);
			return Integer.compare(this.left, o.left);
		}
		return Double.compare(this.score, o.score);	//ascending order
	}

	@Override
	public String toString() {
		return String.format("%s,%s,%s", left, right, score);
	}
}