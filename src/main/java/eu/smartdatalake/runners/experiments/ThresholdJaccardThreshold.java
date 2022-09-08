package eu.smartdatalake.runners.experiments;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.ObjectInputStream;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import eu.smartdatalake.runners.ThresholdCompetitor;
import eu.smartdatalake.simjoin.alg.Algorithm;
import eu.smartdatalake.simjoin.alg.threshold.jaccard.Silkmoth;
import eu.smartdatalake.simjoin.alg.threshold.jaccard.SilkmothF;
import eu.smartdatalake.simjoin.alg.threshold.jaccard.TokenJoin;
import eu.smartdatalake.simjoin.alg.threshold.jaccard.TokenJoinV;
import eu.smartdatalake.simjoin.alg.threshold.jaccard.TokenJoinF;
import eu.smartdatalake.simjoin.alg.threshold.jaccard.Hybrid;
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
//
//		FileOutputStream fileOutputStream;
//		try {
//			fileOutputStream = new FileOutputStream("enron_collection.txt");
//			ObjectOutputStream objectOutputStream = new ObjectOutputStream(fileOutputStream);
//			objectOutputStream.writeObject(collection);
//			objectOutputStream.flush();
//			objectOutputStream.close();
//			fileOutputStream.close();
//		} catch (IOException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}

//		FuzzyIntSetCollection collection = null;
//		try {
//			FileInputStream fileInputStream = new FileInputStream("enron_collection.txt");
//			ObjectInputStream objectInputStream = new ObjectInputStream(fileInputStream);
//			collection = (FuzzyIntSetCollection) objectInputStream.readObject();
//			objectInputStream.close();
//			fileInputStream.close();
//		} catch (IOException | ClassNotFoundException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}

		duration = System.nanoTime() - duration;
//		if (true)
//			return;

		System.out.println("Preparation time: " + duration / 1000000000.0 + " sec.");

//			try {
//				TimeUnit.SECONDS.sleep(30);
//			} catch (InterruptedException e) {
//				// TODO Auto-generated catch block
//				e.printStackTrace();
//			}

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
					alg = new Silkmoth(new ThresholdCompetitor("SM", false, 0));
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
					alg = new SilkmothF(new ThresholdCompetitor("SMF", false, 0));
					break;
				case 7: // SM;
					alg = new Silkmoth(new ThresholdCompetitor("SM-G", true, 0));
					break;
				case 8: // TJB
					alg = new TokenJoin(new ThresholdCompetitor("TJB-L", false, false, 0, false));
					break;
				case 9: // H;
					alg = new Hybrid(new ThresholdCompetitor("H", true, 0));
					break;
				}

				alg.selfJoin(collection, threshold);

			}

		}
	}
}
