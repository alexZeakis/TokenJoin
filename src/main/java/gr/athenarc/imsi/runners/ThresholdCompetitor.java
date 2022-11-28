package gr.athenarc.imsi.runners;

import org.json.simple.JSONObject;

public class ThresholdCompetitor {
	/**
	 * Name of setting for logs.
	 */
	public String method;

	/**
	 * Whether it is a self-join.
	 */
	public boolean self;
	/**
	 * Whether to use globalOrdering for tokens.
	 */
	public boolean globalOrdering;
	/**
	 * Whether to use position filter. It requires globalOrdering=true.
	 */
	public boolean posFilter;
	/**
	 * Whether to use joint utility filter.
	 */
	public boolean jointFilter;

	/**
	 * Which verification algorithm to use: 0 for Hungarian algorithm, 1 for
	 * Hungarian algorithm + Upper Bounds, 2 for Hungarian algorithm + Upper
	 * {@literal &} Lower Bounds.
	 */
	public int verificationAlg;

	/**
	 * Define ThresholdJoin algorithm setting.
	 * 
	 * @param method:          Name of setting for logs.
	 * @param posFilter:       Whether to use position filter. It internally sets
	 *                         globalOrdering=true.
	 * @param jointFilter:     Whether to use joint utility filter.
	 * @param verificationAlg: Which verification algorithm to use: 0 for Hungarian
	 *                         algorithm, 1 for Hungarian algorithm + Upper Bounds,
	 *                         2 for Hungarian algorithm + Upper {@literal &} Lower
	 *                         Bounds.
	 * @param globalOrdering   Whether to use globalOrdering.
	 */
	public ThresholdCompetitor(String method, boolean posFilter, boolean jointFilter, int verificationAlg,
			boolean globalOrdering) {
		this.method = method;
		this.self = true;
		this.globalOrdering = globalOrdering;
		this.posFilter = posFilter;
		this.jointFilter = jointFilter;
		this.verificationAlg = verificationAlg;
	}

	public ThresholdCompetitor(String method, boolean globalOrdering, int verificationAlg) {
		this.method = method;
		this.self = true;
		this.globalOrdering = globalOrdering;
		this.verificationAlg = verificationAlg;
	}

	public ThresholdCompetitor(JSONObject args) {
		this.method = String.valueOf(args.get("method"));
		this.self = true;
		this.globalOrdering = Boolean.parseBoolean(String.valueOf(args.get("globalOrdering")));
		this.posFilter = Boolean.parseBoolean(String.valueOf(args.get("posFilter")));
		this.jointFilter = Boolean.parseBoolean(String.valueOf(args.get("jointFilter")));
		this.verificationAlg = Integer.parseInt(String.valueOf(args.get("verificationAlg")));
	}

	@SuppressWarnings("unchecked")
	public void write(JSONObject log) {
		log.put("name", method);
		JSONObject args = new JSONObject();
		args.put("self", self);
		args.put("globalOrdering", globalOrdering);
		args.put("posFilter", posFilter);
		args.put("jointFilter", jointFilter);
		args.put("verificationAlg", verificationAlg);
		log.put("args", args);
	}
}
