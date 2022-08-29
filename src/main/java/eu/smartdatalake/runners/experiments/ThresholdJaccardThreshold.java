package eu.smartdatalake.runners.experiments;

import java.util.Iterator;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import eu.smartdatalake.runners.ThresholdCompetitor;
import eu.smartdatalake.simjoin.alg.Algorithm;
import eu.smartdatalake.simjoin.alg.threshold.jaccard.Silkmoth;
import eu.smartdatalake.simjoin.alg.threshold.jaccard.SilkmothF;
import eu.smartdatalake.simjoin.alg.threshold.jaccard.TokenJoin;
import eu.smartdatalake.simjoin.alg.threshold.jaccard.TokenJoinV;
import eu.smartdatalake.simjoin.alg.threshold.jaccard.TokenJoinF;
import eu.smartdatalake.simjoin.util.collection.FuzzyIntSetCollection;
import eu.smartdatalake.simjoin.util.collection.FuzzySetCollectionReader;

/**
 * Experiment for Jaccard Threshold.
 *
 */
public class ThresholdJaccardThreshold {
//	protected static final Logger logger = LogManager.getLogger(Silkmoth.class);

	@SuppressWarnings("unchecked")
	public static void run(JSONObject readConfig, JSONObject execConfig) {

		/* EXECUTE THE OPERATION */
		long duration = System.nanoTime();
		FuzzyIntSetCollection collection = new FuzzySetCollectionReader().prepareCollection(readConfig, execConfig,
				false, false);
		duration = System.nanoTime() - duration;
		System.out.println("Preparation time: " + duration / 1000000000.0 + " sec.");

		JSONArray thresholds = (JSONArray) execConfig.get("threshold");
		Iterator<Double> iterator = thresholds.iterator();

		while (iterator.hasNext()) {
			double threshold = iterator.next();

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
					alg = new TokenJoinV(new ThresholdCompetitor("TJPJ - VUL", true, true, 2, true));
					break;
				case 5: // TJF
					alg = new TokenJoinF(new ThresholdCompetitor("TJF", true, true, 2, true));
					break;
				case 6: // SMF
					alg = new SilkmothF(new ThresholdCompetitor("SMF", false));
					break;
				case 7: // SM;
					alg = new Silkmoth(new ThresholdCompetitor("SM-G", true));
					break;
				case 8: // TJB
					alg = new TokenJoin(new ThresholdCompetitor("TJB-L", false, false, 0, false));
					break;					
				}

				alg.selfJoin(collection, threshold);

			}

		}
	}
}
