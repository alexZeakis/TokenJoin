package eu.smartdatalake.runners;

import java.io.IOException;
import java.util.Arrays;

import java.io.FileInputStream;
import java.io.ObjectInputStream;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LoggerContext;

import eu.smartdatalake.simjoin.alg.Algorithm;
import eu.smartdatalake.simjoin.util.collection.FuzzyIntSetCollection;

public class MainRunner {

	public static void main(String[] args) {
		try {

			System.out.println(Arrays.toString(args));
			
			String similarity = "jaccard", logFile = "", inputFile = "", model = "TJ";
			double delta = 0.9;
			int k = 500;
			for (int i = 0; i < args.length; i += 2) {
				if (args[i].equals("--similarity") || args[i].equals("-s"))
					similarity = args[i + 1].toLowerCase();
				if (args[i].equals("--log") || args[i].equals("-l"))
					logFile = args[i + 1];
				if (args[i].equals("--input") || args[i].equals("-i"))
					inputFile = args[i + 1];
				if (args[i].equals("--model") || args[i].equals("-m"))
					model = args[i + 1];
				if (args[i].equals("--delta") || args[i].equals("-d"))
					delta = Double.parseDouble(args[i + 1]);
				if (args[i].equals("--k") || args[i].equals("-k"))
					k = Integer.parseInt(args[i + 1]);
			}

			if (logFile.equals(""))
				throw new IllegalArgumentException("A log directory must be passed!");

			if (inputFile.equals(""))
				throw new IllegalArgumentException("An input file must be passed!");

			String[] similarities = { "jaccard", "edit" };
			if (!Arrays.asList(similarities).contains(similarity))
				throw new IllegalArgumentException(
						"Invalid similarity. Valid values are: " + Arrays.toString(similarities));

			String[] models = { "TJ", "TJP", "TJPJ", "SM", "TJK", "SMK", "FJK" };
			if (!Arrays.asList(model).contains(model))
				throw new IllegalArgumentException("Invalid model. Valid values are: " + Arrays.toString(models));

			long duration = System.nanoTime();
			FuzzyIntSetCollection collection = null;
			try {
				FileInputStream fileInputStream = new FileInputStream(inputFile);
				ObjectInputStream objectInputStream = new ObjectInputStream(fileInputStream);
				collection = (FuzzyIntSetCollection) objectInputStream.readObject();
				objectInputStream.close();
				fileInputStream.close();
			} catch (IOException | ClassNotFoundException e) {
				e.printStackTrace();
			}
			duration = System.nanoTime() - duration;
			System.out.println("Preparation time: " + duration / 1000000000.0 + " sec.");

			System.setProperty("logFilename", logFile);
			LoggerContext ctx = (LoggerContext) LogManager.getContext(false);
			ctx.reconfigure();

			if (similarity.equals("jaccard")) {
				Algorithm alg = null;
				switch (model) {
				case "SM": // SM;
					alg = new eu.smartdatalake.simjoin.alg.threshold.jaccard.Silkmoth(
							new ThresholdCompetitor("SM", false, 0));
					alg.selfJoin(collection, delta);
					break;
				case "TJ": // TJB
					alg = new eu.smartdatalake.simjoin.alg.threshold.jaccard.TokenJoin(
							new ThresholdCompetitor("TJB", false, false, 0, true));
					alg.selfJoin(collection, delta);
					break;
				case "TJP": // TJP
					alg = new eu.smartdatalake.simjoin.alg.threshold.jaccard.TokenJoin(
							new ThresholdCompetitor("TJP", true, false, 0, true));
					alg.selfJoin(collection, delta);
					break;
				case "TJPJ": // TJPJ
					alg = new eu.smartdatalake.simjoin.alg.threshold.jaccard.TokenJoin(
							new ThresholdCompetitor("TJPJ", true, true, 0, true));
					alg.selfJoin(collection, delta);
					break;
				case "TJV": // TJV
					alg = new eu.smartdatalake.simjoin.alg.threshold.jaccard.TokenJoinV(
							new ThresholdCompetitor("TJPJ - VUL", true, true, 2, true));
					alg.selfJoin(collection, delta);
					break;
				case "TJK": // TJV
					alg = new eu.smartdatalake.simjoin.alg.topk.jaccard.TokenJoin(
							new TopkCompetitor("TJK", 2, 0.90, 0.01, 2, 0.4));
					alg.selfJoin(collection, k);
					break;
				case "OT":
					new eu.smartdatalake.simjoin.alg.topk.jaccard.Fixed().selfJoinFixed(collection, k, delta);
					break;
				case "SMK":
					alg = new eu.smartdatalake.simjoin.alg.topk.jaccard.Silkmoth(
							new TopkCompetitor("SMK", 2, 0.90, 0.01, 2, 0.4));
					alg.selfJoin(collection, k);
					break;
				case "FJK":
					alg = new eu.smartdatalake.simjoin.alg.topk.jaccard.TopkJoin(
							new TopkCompetitor("FJK", 2, 0.90, 0.01, 2, 0.4));
					alg.selfJoin(collection, k);
					break;
				}
			} else if (similarity.equals("edit")) {
				Algorithm alg = null;
				switch (model) {
				case "SM": // SM;
					alg = new eu.smartdatalake.simjoin.alg.threshold.edit.Silkmoth(
							new ThresholdCompetitor("SM", false, 0));
					alg.selfJoin(collection, delta);
					break;
				case "TJ": // TJB
					alg = new eu.smartdatalake.simjoin.alg.threshold.edit.TokenJoin(
							new ThresholdCompetitor("TJB", false, false, 0, true));
					alg.selfJoin(collection, delta);
					break;
				case "TJP": // TJP
					alg = new eu.smartdatalake.simjoin.alg.threshold.edit.TokenJoin(
							new ThresholdCompetitor("TJP", true, false, 0, true));
					alg.selfJoin(collection, delta);
					break;
				case "TJPJ": // TJPJ
					alg = new eu.smartdatalake.simjoin.alg.threshold.edit.TokenJoin(
							new ThresholdCompetitor("TJPJ", true, true, 0, true));
					alg.selfJoin(collection, delta);
					break;
				case "TJV": // TJV
					alg = new eu.smartdatalake.simjoin.alg.threshold.edit.TokenJoinV(
							new ThresholdCompetitor("TJPJ - VUL", true, true, 2, true));
					alg.selfJoin(collection, delta);
					break;
				case "TJK": // TJV
					alg = new eu.smartdatalake.simjoin.alg.topk.edit.TokenJoin(
							new TopkCompetitor("TJK", 2, 0.90, 0.01, 2, 0.4));
					alg.selfJoin(collection, k);
					break;
				case "OT":
					new eu.smartdatalake.simjoin.alg.topk.edit.Fixed().selfJoinFixed(collection, k, delta);
					break;
				case "SMK":
					alg = new eu.smartdatalake.simjoin.alg.topk.edit.Silkmoth(
							new TopkCompetitor("SMK", 2, 0.90, 0.01, 2, 0.4));
					alg.selfJoin(collection, k);
					break;
				case "FJK":
					alg = new eu.smartdatalake.simjoin.alg.topk.edit.TopkJoin(
							new TopkCompetitor("FJK", 2, 0.90, 0.01, 2, 0.4));
					alg.selfJoin(collection, k);
					break;
				}
			}
		} finally {

		}
	}
}