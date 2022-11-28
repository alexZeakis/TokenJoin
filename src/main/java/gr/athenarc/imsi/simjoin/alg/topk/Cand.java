package gr.athenarc.imsi.simjoin.alg.topk;

/**
 * Class for storing candidates with a current Upper Bound.
 *
 */
public class Cand implements Comparable<Cand> {
	public int id, stage;
	public double utilGathered, score;

	public Cand(int id, double utilGathered, double score) {
		this.id = id;
		this.utilGathered = utilGathered;
		this.stage = 0;
		this.score = score;
	}

	@Override
	public int compareTo(Cand o) {
		if (this.score == o.score) {
			return Integer.compare(this.id, o.id);
		}
		return Double.compare(o.score, this.score); // descending order
	}

	@Override
	public String toString() {
		return String.format("%s->(%s,%s,%d)", id, utilGathered, score, stage);
	}
}