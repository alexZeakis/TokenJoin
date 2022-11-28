package gr.athenarc.imsi.runners;

import org.json.simple.JSONObject;

public class TopkCompetitor {
	/**
	 * Name of setting for logs.
	 */
	public String method;
	/**
	 * Choice for Threshold Initialization: 0 is for 0.0, 1 is for random
	 * verifications and 2 for suggested method.
	 */
	public int choice;

	/**
	 * For δ-Initialization: Number of candidates to refine per record. (μ * k)
	 */
	public double mu;

	/**
	 * For δ-Initialization: Number of verifications to perform. (λ * k)
	 */
	public int lambda;

	/**
	 * For δ-Initialization: δ To use for Candidate Generation
	 */
	public double deltaGeneration;

	/**
	 * For δ-Initialization: Use percentage of collection as query records.
	 */
	public double samplePercentage;

	/**
	 * @param method:           Name of setting for logs.
	 * @param choice:           Choice for Threshold Initialization: 0 is for 0.0, 1
	 *                          is for random verifications and 2 for suggested
	 *                          method.
	 * @param deltaGeneration:  For δ-Initialization: δ To use for Candidate
	 *                          Generation
	 * @param mu:               For δ-Initialization: Top-μ*k to keep from each record
	 * @param lambda:           For δ-Initialization: Top-λ*k to keep from all records
	 * @param samplePercentage: For δ-Initialization: Use percentage of collection
	 *                          as query records.
	 */
	public TopkCompetitor(String method, int choice, double deltaGeneration, double mu, int lambda,
			double samplePercentage) {
		this.method = method;
		this.deltaGeneration = deltaGeneration;

		this.mu = mu;
		this.lambda = lambda;
		this.choice = choice;
		this.samplePercentage = samplePercentage;
	}

	/**
	 * @param method: Name of setting for logs.
	 * @param choice: Choice for Threshold Initialization: 0 is for 0.0, 1 is for
	 *                random verifications and 2 for suggested method.
	 */
	public TopkCompetitor(String method, int choice) {
		this.method = method;
		this.choice = choice;
	}

	public TopkCompetitor(JSONObject args) {
		this.method = String.valueOf(args.get("method"));
		this.deltaGeneration = Double.parseDouble(String.valueOf(args.get("deltaGeneration")));

		this.mu = Double.parseDouble(String.valueOf(args.get("mu")));
		this.lambda = Integer.parseInt(String.valueOf(args.get("lambda")));
		this.choice = Integer.parseInt(String.valueOf(args.get("choice")));

		this.samplePercentage = Double.parseDouble(String.valueOf(args.get("samplePercentage")));
	}

	@SuppressWarnings("unchecked")
	public void write(JSONObject log) {
		log.put("name", method);
		JSONObject args = new JSONObject();
		args.put("choice", choice);
		args.put("mu", mu);
		args.put("lambda", lambda);
		args.put("deltaGeneration", deltaGeneration);
		args.put("samplePercentage", samplePercentage);

		log.put("args", args);
	}
}
