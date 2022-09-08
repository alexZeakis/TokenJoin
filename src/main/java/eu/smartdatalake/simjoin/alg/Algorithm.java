package eu.smartdatalake.simjoin.alg;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.simple.JSONObject;

import eu.smartdatalake.simjoin.util.GraphVerifier;
import eu.smartdatalake.simjoin.util.collection.FuzzyIntSetCollection;

/**
 * General class for algorithm implementation.
 *
 */
public class Algorithm {

	protected long startTime, joinTime, signatureGenerationTime = 0, indexTime = 0;

	protected long candGenTime = 0, CFTime = 0, initTime = 0;
	protected long jointTime = 0, NNFTime = 0;
	protected long phase1Time = 0, phase2Time = 0, verificationTime = 0;

	protected long candGenands = 0, CFCands = 0, JointCands = 0, NNFCands = 0, refineCands = 0;
	protected long phase1Cands = 0, phase2Cands = 0, verifiable = 0, totalMatches = 0;
	protected static final Logger logger = LogManager.getLogger(Algorithm.class);

	protected long threshTime = 0;

	protected JSONObject log;

//	protected long timeOut = 18000;
	protected long timeOut = 172800;

	protected long LimitReject = 500000000, noReject = 0;

	public void selfJoin(FuzzyIntSetCollection collection, double threshold) {

	}

	@SuppressWarnings("unchecked")
	protected void write_times_terms(JSONObject log) {
		JSONObject times = new JSONObject();
		times.put("total", joinTime / 1000000000.0);
		times.put("signature_generation", signatureGenerationTime / 1000000000.0);
		times.put("index", indexTime / 1000000000.0);
		times.put("phase_1", phase1Time / 1000000000.0);
		times.put("phase_2", phase2Time / 1000000000.0);
		times.put("joint", jointTime / 1000000000.0);
		times.put("nnf", NNFTime / 1000000000.0);
		times.put("verification", verificationTime / 1000000000.0);
		log.put("times", times);

		JSONObject terms = new JSONObject();
		terms.put("candGen", candGenands);
		terms.put("candRef", refineCands);
		terms.put("nnf", NNFCands);
		terms.put("verifiable", verifiable);
		terms.put("total", totalMatches);
		log.put("terms", terms);
	}

	@SuppressWarnings("unchecked")
	protected void writeSilkmothStats(JSONObject log) {

		phase1Time = candGenTime;
		phase2Time = initTime + CFTime + NNFTime;

		JSONObject times = new JSONObject();
		times.put("total", joinTime / 1000000000.0);
		times.put("signature_generation", signatureGenerationTime / 1000000000.0);
		times.put("index", indexTime / 1000000000.0);
		times.put("phase_1", phase1Time / 1000000000.0);
		times.put("cand_gen", candGenTime / 1000000000.0);
		times.put("init", initTime / 1000000000.0);
		times.put("check_filter", CFTime / 1000000000.0);
		times.put("phase_2", phase2Time / 1000000000.0);
		times.put("nnf", NNFTime / 1000000000.0);
		times.put("verification", verificationTime / 1000000000.0);
		log.put("times", times);

		JSONObject terms = new JSONObject();
		terms.put("candGen", candGenands);
		terms.put("check_filter", CFCands);
		terms.put("nnf", NNFCands);
		terms.put("verifiable", verifiable);
		terms.put("total", totalMatches);
		log.put("terms", terms);
	}

	protected void initVerificationTerms() {
		GraphVerifier.initTerms();
	}

	protected void writeVerificationTerms(JSONObject log) {
		GraphVerifier.writeTerms(log, verificationTime);
	}
}
