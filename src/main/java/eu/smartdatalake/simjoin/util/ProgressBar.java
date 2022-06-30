package eu.smartdatalake.simjoin.util;


/**
 * Util Function for checking progress of algorithm.
 *
 */
public class ProgressBar {
	int totalSteps;
	int step;
	int len;
	int count;

	public ProgressBar(int totalSteps, int len) {
		this.totalSteps = totalSteps;
		this.len = len;
		step = len / totalSteps;
		count = 0;
	}

	public ProgressBar(int len) {
		this(20, len);
	}

	public void progress(long time) {
		if (len >= 100) {
			count++;
			if (len >= totalSteps) {
				if (count % step == 0) {
					long now = System.nanoTime();
					double elapsed = (now - time) / 1000000000.0;
					double eta = (elapsed * step * totalSteps) / count - elapsed;
					String msg = String.format("%d%% \tElapsed: %dm %ds  \t\tETA: %dm %ds\r",
							(count / step * 100) / totalSteps, (int) (elapsed / 60.0), (int) (elapsed % 60.0),
							(int) (eta / 60.0), (int) (eta % 60.0));
					System.out.print(msg);
				}
			}
		}
	}
	
	public void progressK(long time, double threshold) {
		if (len >= 100) {
			count++;
			if (len >= totalSteps) {
				if (count % step == 0) {
					long now = System.nanoTime();
					double elapsed = (now - time) / 1000000000.0;
					double eta = (elapsed * step * totalSteps) / count - elapsed;
					String msg = String.format("%d%% \tElapsed: %dm %ds  \t\tETA: %dm %ds \tThreshold: %.2f\r",
							(count / step * 100) / totalSteps, (int) (elapsed / 60.0), (int) (elapsed % 60.0),
							(int) (eta / 60.0), (int) (eta % 60.0), threshold);
					System.out.print(msg);
				}
			}
		}
	}	
}
