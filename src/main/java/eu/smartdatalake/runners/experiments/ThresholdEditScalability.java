package eu.smartdatalake.runners.experiments;

import java.util.Iterator;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import eu.smartdatalake.runners.ThresholdCompetitor;
import eu.smartdatalake.simjoin.alg.Algorithm;
import eu.smartdatalake.simjoin.alg.threshold.edit.Silkmoth;
import eu.smartdatalake.simjoin.alg.threshold.edit.TokenJoin;
import eu.smartdatalake.simjoin.util.collection.FuzzyIntSetCollection;
import eu.smartdatalake.simjoin.util.collection.FuzzySetCollectionReader;

/**
 * Experiment for Edit Scalability.
 *
 */
public class ThresholdEditScalability {

	@SuppressWarnings("unchecked")
	public static void run(JSONObject readConfig, JSONObject execConfig) {

		/* EXECUTE THE OPERATION */

		int totalLines = Integer.parseInt(String.valueOf(execConfig.get("total_lines")));

		for (double perc = 0.2; perc < 1.1; perc += 0.2) {
			long duration = System.nanoTime();
			int maxLines = (int) (perc * totalLines);
			execConfig.put("max_lines", maxLines);

			FuzzyIntSetCollection collection = new FuzzySetCollectionReader().prepareCollection(readConfig, execConfig,
					true, false);
			duration = System.nanoTime() - duration;
			System.out.println("Preparation time: " + duration / 1000000000.0 + " sec.");

			double threshold = Double.parseDouble(String.valueOf(execConfig.get("threshold")));

			JSONArray models = (JSONArray) execConfig.get("models");
			Iterator<Long> modelIterator = models.iterator();
			while (modelIterator.hasNext()) {
				int model = modelIterator.next().intValue();

				Algorithm alg = null;
				switch (model) {
				case 0: // SM;
					alg = new Silkmoth(new ThresholdCompetitor("SM", false));
					break;
				case 1: // TJB
					alg = new TokenJoin(new ThresholdCompetitor("TJB", false, false, 0, true));
					break;
				case 2: // TJP
					alg = new TokenJoin(new ThresholdCompetitor("TJP", true, false, 0, true));
					break;
				case 3: // TJPJ
					alg = new TokenJoin(new ThresholdCompetitor("TJPJ", true, true, 0, true));
					break;
				case 4: // TJV
					alg = new TokenJoin(new ThresholdCompetitor("TJPJ - VUL", true, true, 2, true));
					break;
				}

				alg.selfJoin(collection, threshold);
			}
		}
	}

}
