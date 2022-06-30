package eu.smartdatalake.runners.experiments;

import java.util.Iterator;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import eu.smartdatalake.runners.ThresholdCompetitor;
import eu.smartdatalake.simjoin.alg.Algorithm;
import eu.smartdatalake.simjoin.alg.threshold.edit.Silkmoth;
import eu.smartdatalake.simjoin.alg.threshold.edit.TokenJoin;
import eu.smartdatalake.simjoin.alg.threshold.edit.TokenJoinV;
import eu.smartdatalake.simjoin.util.collection.FuzzyIntSetCollection;
import eu.smartdatalake.simjoin.util.collection.FuzzySetCollectionReader;

/**
 * Experiment for Edit Threshold.
 *
 */
public class ThresholdEditThreshold {

	@SuppressWarnings("unchecked")
	public static void run(JSONObject readConfig, JSONObject execConfig) {

		/* EXECUTE THE OPERATION */
		long duration = System.nanoTime();
		FuzzyIntSetCollection collection = new FuzzySetCollectionReader().prepareCollection(readConfig, execConfig,
				true, false);
		duration = System.nanoTime() - duration;
		System.out.println("Preparation time: " + duration / 1000000000.0 + " sec.");

		JSONArray thresholds = (JSONArray) execConfig.get("threshold");
		Iterator<Double> thresholdIterator = thresholds.iterator();

		while (thresholdIterator.hasNext()) {
			double threshold = thresholdIterator.next();

			JSONArray models = (JSONArray) execConfig.get("models");
			Iterator<Long> modelIterator = models.iterator();
			while (modelIterator.hasNext()) {
				int model = modelIterator.next().intValue();
				
				Algorithm alg = null;
				switch (model) {
				case 0: // SM;
					alg = new Silkmoth(new ThresholdCompetitor("SM"));
					break;
				case 1: // TJB
					alg = new TokenJoin(new ThresholdCompetitor("TJB", false, false, 0));
					break;
				case 2: // TJP
					alg = new TokenJoin(new ThresholdCompetitor("TJP", true, false, 0));
					break;
				case 3: // TJPJ
					alg = new TokenJoin(new ThresholdCompetitor("TJPJ", true, true, 0));
					break;
				case 4: // TJV
					alg = new TokenJoinV(new ThresholdCompetitor("TJPJ - VUL", true, true, 2));
					break;					
				}
				
				alg.selfJoin(collection, threshold);
			}
		}
	}

}
