package eu.smartdatalake.runners;

import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LoggerContext;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import eu.smartdatalake.runners.experiments.ThresholdEditScalability;
import eu.smartdatalake.runners.experiments.ThresholdEditThreshold;
import eu.smartdatalake.runners.experiments.TopKEditK;
import eu.smartdatalake.runners.experiments.ThresholdJaccardScalability;
import eu.smartdatalake.runners.experiments.ThresholdJaccardThreshold;
import eu.smartdatalake.runners.experiments.TopKJaccardK;
import eu.smartdatalake.simjoin.util.collection.FuzzyIntSetCollection;
import eu.smartdatalake.simjoin.util.collection.FuzzySetCollectionReader;



public class MainRunner {

	@SuppressWarnings("unchecked")
	public static void main(String[] args) {
		try {

			String configFile = "", similarity = "jaccard", type = "threshold", mode = "single", log_dir="", input_dir="";
			for (int i = 0; i < args.length; i += 2) {
				if (args[i].equals("--config") || args[i].equals("-c"))
					configFile = args[i + 1];
				if (args[i].equals("--similarity") || args[i].equals("-s"))
					similarity = args[i + 1].toLowerCase();
				if (args[i].equals("--type") || args[i].equals("-t"))
					type = args[i + 1].toLowerCase();
				if (args[i].equals("--mode") || args[i].equals("-m"))
					mode = args[i + 1].toLowerCase();
				if (args[i].equals("--log") || args[i].equals("-l"))
					log_dir = args[i + 1];
				if (args[i].equals("--input") || args[i].equals("-i"))
					input_dir = args[i + 1];				
			}

			if (configFile.equals(""))
				throw new IllegalArgumentException("A config file must be passed!");

			if (log_dir.equals(""))
				throw new IllegalArgumentException("A log directory must be passed!");			

			if (input_dir.equals(""))
				throw new IllegalArgumentException("An input directory must be passed!");				
			
			String[] similarities = { "jaccard", "edit" };
			if (!Arrays.asList(similarities).contains(similarity))
				throw new IllegalArgumentException(
						"Invalid similarity. Valid values are: " + Arrays.toString(similarities));

			String[] types = { "threshold", "topk", "threshold_scalability", "threshold_verification",
					"threshold_threshold", "threshold_filtering" };
			if (type != null && !Arrays.asList(types).contains(type))
				throw new IllegalArgumentException("Invalid type. Valid values are: " + Arrays.toString(types));

			String[] modes = { "experiment", "single" };
			if (!Arrays.asList(modes).contains(mode))
				throw new IllegalArgumentException("Invalid mode. Valid values are: " + Arrays.toString(modes));

			System.out.println(configFile + " " + similarity + " " + type + " " + mode + ".");

			/* READ PARAMETERS */
			JSONParser jsonParser = new JSONParser();
			JSONObject config = (JSONObject) jsonParser.parse(new FileReader(configFile));

			JSONObject execConfig = (JSONObject) config.get("execute");
			execConfig = (JSONObject) execConfig.get(mode);
			execConfig = (JSONObject) execConfig.get(type);

			JSONObject readConfig = (JSONObject) config.get("read");
			readConfig.put("input_file", input_dir + (String) readConfig.get("input_file"));

			String name = String.valueOf(config.get("name"));

			if (mode.equals("experiment")) {
				String logFile = String.format("%s%s/%s/%s.log", log_dir, mode, type, name);
				System.setProperty("logFilename", logFile);
				LoggerContext ctx = (LoggerContext) LogManager.getContext(false);
				ctx.reconfigure();

//				execConfig = (JSONObject) execConfig.get(type);

				if (similarity.equals("jaccard")) {
					if (type.equals("threshold_threshold") || type.equals("threshold_verification") || type.equals("threshold_filtering")) {
						ThresholdJaccardThreshold.run(readConfig, execConfig);
					} else if (type.equals("threshold_scalability")) {
						ThresholdJaccardScalability.run(readConfig, execConfig);
					} else if (type.equals("topk")) {
						TopKJaccardK.run(readConfig, execConfig);
					}
				} else if (similarity.equals("edit")) {
					if (type.equals("threshold_threshold") || type.equals("threshold_verification") || type.equals("threshold_filtering")) {
						ThresholdEditThreshold.run(readConfig, execConfig);
					} else if (type.equals("threshold_scalability")) {
						ThresholdEditScalability.run(readConfig, execConfig);
					} else if (type.equals("topk")) {
						TopKEditK.run(readConfig, execConfig);
					}
				}
			} else if (mode.equals("single")) {
				String logFile = String.format("%s%s/%s/%s/%s.log", log_dir, mode, type, similarity, name);
				System.setProperty("logFilename", logFile);
				LoggerContext ctx = (LoggerContext) LogManager.getContext(false);
				ctx.reconfigure();

				execConfig = (JSONObject) execConfig.get(similarity);

				if (type.equals("threshold")) {
					JSONObject thresholdArgs = (JSONObject) execConfig.get("arguments");
					ThresholdCompetitor c = new ThresholdCompetitor(thresholdArgs);

					long duration = System.nanoTime();
					FuzzyIntSetCollection collection = new FuzzySetCollectionReader().prepareCollection(readConfig,
							execConfig, similarity.equals("edit"), false);
					duration = System.nanoTime() - duration;
					System.out.println("Preparation time: " + duration / 1000000000.0 + " sec.");

					String algorithm = String.valueOf(execConfig.get("algorithm"));
					double threshold = Double.parseDouble(String.valueOf(execConfig.get("threshold")));

					if (similarity.equals("jaccard")) {
						if (algorithm.equals("tokenjoin")) {
							new eu.smartdatalake.simjoin.alg.threshold.jaccard.TokenJoin(c).selfJoin(collection,
									threshold);
						} else if (algorithm.equals("silkmoth")) {
							new eu.smartdatalake.simjoin.alg.threshold.jaccard.Silkmoth(c).selfJoin(collection,
									threshold);
						}

					} else if (similarity.equals("edit")) {
						if (algorithm.equals("tokenjoin")) {
							new eu.smartdatalake.simjoin.alg.threshold.edit.TokenJoin(c).selfJoin(collection,
									threshold);
						} else if (algorithm.equals("silkmoth")) {
							new eu.smartdatalake.simjoin.alg.threshold.edit.Silkmoth(c).selfJoin(collection, threshold);
						}
					}

				} else if (type.equals("topk")) {
					JSONObject topkArgs = (JSONObject) execConfig.get("arguments");
					TopkCompetitor c = new TopkCompetitor(topkArgs);

					long duration = System.nanoTime();
					FuzzyIntSetCollection collection = new FuzzySetCollectionReader().prepareCollection(readConfig,
							execConfig, similarity.equals("edit"), true);
					duration = System.nanoTime() - duration;
					System.out.println("Preparation time: " + duration / 1000000000.0 + " sec.");

					String algorithm = String.valueOf(execConfig.get("algorithm"));
					int k = Integer.parseInt(String.valueOf(execConfig.get("k")));

					if (similarity.equals("jaccard")) {
						if (algorithm.equals("tokenjoin")) {
							new eu.smartdatalake.simjoin.alg.topk.jaccard.TokenJoin(c).selfJoin(collection, k);
						} else if (algorithm.equals("silkmoth")) {
							new eu.smartdatalake.simjoin.alg.topk.jaccard.Silkmoth(c).selfJoin(collection, k);
						} else if (algorithm.equals("topkjoin")) {
							new eu.smartdatalake.simjoin.alg.topk.jaccard.TopkJoin(c).selfJoin(collection, k);
						} else if (algorithm.equals("best")) {
							double threshold = Double.parseDouble(String.valueOf(execConfig.get("threshold")));
							new eu.smartdatalake.simjoin.alg.topk.jaccard.Fixed().selfJoinFixed(collection, k,
									threshold);
						}
					} else if (similarity.equals("edit")) {
						if (algorithm.equals("tokenjoin")) {
							new eu.smartdatalake.simjoin.alg.topk.edit.TokenJoin(c).selfJoin(collection, k);
						} else if (algorithm.equals("silkmoth")) {
							new eu.smartdatalake.simjoin.alg.topk.edit.Silkmoth(c).selfJoin(collection, k);
						} else if (algorithm.equals("topkjoin")) {
							new eu.smartdatalake.simjoin.alg.topk.edit.TopkJoin(c).selfJoin(collection, k);
						} else if (algorithm.equals("best")) {
							double threshold = Double.parseDouble(String.valueOf(execConfig.get("threshold")));
							new eu.smartdatalake.simjoin.alg.topk.edit.Fixed().selfJoinFixed(collection, k, threshold);
						}
					}

				} else {
					throw new IllegalArgumentException("Wrong sinle type.");
				}

			}
		} catch (ParseException |

				IOException e) {
			e.printStackTrace();
		}
	}
}