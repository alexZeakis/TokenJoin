package eu.smartdatalake.runners.experiments;

import java.util.Iterator;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import eu.smartdatalake.runners.TopkCompetitor;
import eu.smartdatalake.simjoin.alg.topk.jaccard.Fixed;
import eu.smartdatalake.simjoin.alg.topk.jaccard.TopkJoin;
import eu.smartdatalake.simjoin.alg.topk.jaccard.Silkmoth;
import eu.smartdatalake.simjoin.alg.topk.jaccard.TokenJoin;
import eu.smartdatalake.simjoin.util.collection.FuzzyIntSetCollection;
import eu.smartdatalake.simjoin.util.collection.FuzzySetCollectionReader;


/**
 * Experiment for Jaccard TopK.
 *
 */
public class TopKJaccardK {
	protected static final Logger logger = LogManager.getLogger(TopKJaccardK.class);

	public static void run(JSONObject readConfig, JSONObject execConfig) {

		/* EXECUTE THE OPERATION */
		long duration = System.nanoTime();
		FuzzyIntSetCollection collection = new FuzzySetCollectionReader().prepareCollection(readConfig, execConfig,
				false, true);
		duration = System.nanoTime() - duration;
		System.out.println("Preparation time: " + duration / 1000000000.0 + " sec.");

		JSONArray ks = (JSONArray) execConfig.get("k");
		@SuppressWarnings("unchecked")
		Iterator<Long> iterator = ks.iterator();
		while (iterator.hasNext()) {
			int k = iterator.next().intValue();

			System.out.println("\n\nK is " + k);

			double threshold = 0.0;

			TopkCompetitor c = new TopkCompetitor("TJK", 2, 0.90, 0.01, 2, 0.4);
			threshold = new TokenJoin(c).selfJoin(collection, k);

			threshold = new Fixed().selfJoinFixed(collection, k, threshold);

			TopkCompetitor c2 = new TopkCompetitor("SMK", 2, 0.90, 0.01, 2, 0.4);
			threshold = new Silkmoth(c2).selfJoin(collection, k);
			TopkCompetitor c3 = new TopkCompetitor("FJK", 2, 0.90, 0.01, 2, 0.4);
			threshold = new TopkJoin(c3).selfJoin(collection, k);

		}
	}
}