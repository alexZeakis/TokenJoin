package eu.smartdatalake.simjoin.util;

import org.apache.commons.text.similarity.LevenshteinDistance;

/**
 * Class for computing element similarities.
 *
 */
public class Verification {

	/**
	 * @param r: left set of tokens
	 * @param s: right set of tokens
	 * @return Jaccard Similarity of r and s.
	 */
	public static double verifyWithScore(int[] r, int[] s) {

		int olap = 0, pr = 0, ps = 0;
		int maxr = r.length - pr + olap;
		int maxs = s.length - ps + olap;

		while (maxr > olap && maxs > olap) {

			if (r[pr] == s[ps]) {
				pr++;
				ps++;
				olap++;
			} else if (r[pr] < s[ps]) {
				pr++;
				maxr--;
			} else {
				ps++;
				maxs--;
			}
		}

		return (double) (olap / (1.0 * (r.length + s.length - olap)));
	}

	/**
	 * @param r: Left String
	 * @param s: Right String
	 * @return Edit Similarity (NEDS) of r and s.
	 */
	public static double verifyWithScore(String r, String s) {
		LevenshteinDistance ld = new LevenshteinDistance();
		int ed = ld.apply(r, s);
		return 1 - 1.0 * ed / Math.max(r.length(), s.length());
	}
}