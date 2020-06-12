/*
 * Copyright (c) 2020 Georgios Damaskinos
 * All rights reserved.
 * @author Georgios Damaskinos <georgios.damaskinos@gmail.com>
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class Itrust {

	public static ConcurrentHashMap<Integer, Hashtable<Integer, Double>> itemProfiles = new ConcurrentHashMap<Integer, Hashtable<Integer, Double>>();
	public static ConcurrentHashMap<Integer, ConcurrentHashMap<Integer, Double>> delta_itemProfiles = new ConcurrentHashMap<Integer, ConcurrentHashMap<Integer, Double>>();
	public static ConcurrentHashMap<Integer, List<Integer>> KNN = new ConcurrentHashMap<Integer, List<Integer>>();

	static double[][] PIJ;
	static double[] QI;
	static double[][] incrSim;
	static double[][] nonIncrSim;
	static int[][] simCount;
	static int[] itemRatingCount;
	static int maxItemID;

	public static long tmin;
	public static long tmax;
	public static double alpha;
	public static int K;

	public static void main(String[] args) throws Exception {
		
		if (args.length < 2) {
			System.out.println(
					"Usage: java Itrust /local/path/to/trainingSet.csv /local/path/to/testSet.csv");
			System.exit(0);
		}
		String trainPath = args[0];
		String testPath = args[1];
		
		// timestamp -> [Rating1, Rating2, ...]
		LinkedHashMap<Long, ArrayList<Rating>> data = parseData(trainPath);

		K = 150;
		alpha = 0.3;
		PIJ = new double[maxItemID + 1][maxItemID + 1];
		QI = new double[maxItemID + 1];
		incrSim = new double[maxItemID + 1][maxItemID + 1];
		nonIncrSim = new double[maxItemID + 1][maxItemID + 1];
		simCount = new int[maxItemID + 1][maxItemID + 1];
		itemRatingCount = new int[maxItemID + 1];
		
		int ratingCount = 0;
		long startTime = System.currentTimeMillis();
		
		ArrayList<Long> sortedTimestamps = new ArrayList<Long>(data.keySet());
		Collections.sort(sortedTimestamps);
		
		System.out.println("Minimum timestamp: " + sortedTimestamps.get(0));
		System.out.println("Maximum timestamp: " + sortedTimestamps.get(sortedTimestamps.size() - 1));
		
		// BOOTSTRAP
		for (Long timestamp : sortedTimestamps) {

			for (Rating rate : data.get(timestamp)) {
				if ((ratingCount + 1) % 1000 == 0) {

					// Update similarities
					for (int id1 : delta_itemProfiles.keySet()) {
						updateQI(id1);
					}

					for (int id1 : delta_itemProfiles.keySet()) {
						for (int id2 : delta_itemProfiles.keySet()) {
							if (id1 < id2) {
								updatePIJ(id1, id2);

							}
						}
					}

					// Add delta profiles to item profiles
					for (int id1 : delta_itemProfiles.keySet()) {
						for (int id : delta_itemProfiles.get(id1).keySet()) {
							itemProfiles.get(id1).put(id, delta_itemProfiles.get(id1).get(id));
						}
						delta_itemProfiles.remove(id1);
					}

				}

				if (!itemProfiles.keySet().contains(rate.getItemId()))
					itemProfiles.put(rate.getItemId(), new Hashtable<Integer, Double>());
				// itemProfiles.get(rate.getRid()).put(rate.getUid(), rate.getRate());

				if (!delta_itemProfiles.keySet().contains(rate.getItemId()))
					delta_itemProfiles.put(rate.getItemId(), new ConcurrentHashMap<Integer, Double>());
				delta_itemProfiles.get(rate.getItemId()).put(rate.getUserId(), rate.getRating());

				ratingCount++;
			}
		}

		System.out.println("Total time taken for bootstrapping: " + (System.currentTimeMillis() - startTime) + " ms");


		for (int i = 0; i < maxItemID + 1; i++) {
			for (int j = 0; j < maxItemID + 1; j++) {
				if (QI[i] == 0 || QI[j] == 0)
					incrSim[i][j] = 0;
				else
					incrSim[i][j] = PIJ[i][j] / (double) (Math.sqrt(QI[i]) * Math.sqrt(QI[j]));
			}
		}

		// nonIncrementalKNN();
		incrementalKNN();

		// Predict Trust-distrust
		// Get test set
		int hitCount = 0;
		int trustElecCount = 0;
		int distrustElecCount = 0;
		int trustVoterCount = 0;
		int distrustVoterCount = 0;
		int predict = 0;
		int newItemCount = 0;
		int MAE = 0;
		int MAECount = 0;
		BufferedReader br = new BufferedReader(new FileReader(testPath));

		int totalCount = 0;
		String line;
		while ((line = br.readLine()) != null) {
			Integer uid = Integer.parseInt(line.substring(0, line.indexOf(",")));
			line = line.substring(line.indexOf(",") + 1);
			Integer rid = Integer.parseInt(line.substring(0, line.indexOf(",")));
			line = line.substring(line.indexOf(",") + 1);
			Double rating = Double.parseDouble(line.substring(0, line.indexOf(",")));
			line = line.substring(line.indexOf(",") + 1);

			trustElecCount = 0;
			distrustElecCount = 0;
			trustVoterCount = 0;
			distrustVoterCount = 0;
			predict = 0;

			if (KNN.get(uid) == null) {
				newItemCount++;
				continue;
			}

			// Check
			if (KNN.get(rid) == null) {
				newItemCount++;
				continue;
			}

			for (int i : KNN.get(rid)) {
				if (itemProfiles.get(i).get(uid) == null)
					continue;
				if (itemProfiles.get(i).get(uid) > 0)
					trustVoterCount++;
				if (itemProfiles.get(i).get(uid) < 0)
					distrustVoterCount++;
			}

			if (trustVoterCount >= distrustVoterCount)
				predict = 1;
			if (distrustVoterCount > trustVoterCount)
				predict = -1;

			MAE += Math.abs(predict - rating);
			MAECount++;
			if (predict == rating && rating != 0)
				hitCount++;
			if (rating != 0)
				totalCount++;

		}
		br.close();

		System.out.println("HitCount: " + hitCount);
		System.out.println("TotalCount: " + totalCount);
		System.out.println("Error: " + (MAE / (double) MAECount));
		System.out.println("New items: " + newItemCount);
	}

	private static Map<Integer, Double> sortByComparator(Map<Integer, Double> unsortMap, final boolean order) {

		List<Entry<Integer, Double>> list = new LinkedList<Entry<Integer, Double>>(unsortMap.entrySet());

		// Sorting the list based on values
		Collections.sort(list, new Comparator<Entry<Integer, Double>>() {
			public int compare(Entry<Integer, Double> o1, Entry<Integer, Double> o2) {
				if (order) {
					return o1.getValue().compareTo(o2.getValue());
				} else {
					return o2.getValue().compareTo(o1.getValue());

				}
			}
		});

		// Maintaining insertion order with the help of LinkedList
		Map<Integer, Double> sortedMap = new LinkedHashMap<Integer, Double>();
		for (Entry<Integer, Double> entry : list) {
			sortedMap.put(entry.getKey(), entry.getValue());
		}

		return sortedMap;
	}

	private static double getCosineSimilarity(Hashtable<Integer, Double> IP1, Hashtable<Integer, Double> IP2, int id1,
			int id2) throws IOException {
		double sim = 0;
		Set<Integer> setIP1 = new HashSet<Integer>();
		Set<Integer> setIP2 = new HashSet<Integer>();

		setIP1.addAll(IP1.keySet());
		setIP2.addAll(IP2.keySet());

		double P_ij = 0, Q_i = 0, Q_j = 0;

		Set<Integer> common = new HashSet<Integer>(setIP1);

		common.retainAll(setIP2);

		for (int id : common) {
			P_ij += IP1.get(id) * IP2.get(id);
		}

		for (int id : setIP1) {
			Q_i += Math.pow(IP1.get(id), 2);
		}

		for (int id : setIP2) {
			Q_j += Math.pow(IP2.get(id), 2);
		}

		if (common.size() == 0)
			return 0.0;
		sim = P_ij / (double) (Math.sqrt(Q_i) * Math.sqrt(Q_j));
		return sim;
	}

	@SuppressWarnings("unused")
	private static void computePIJ(ConcurrentHashMap<Integer, Double> IP1, ConcurrentHashMap<Integer, Double> IP2,
			int id1, int id2) {
		Set<Integer> setIP1 = new HashSet<Integer>();
		Set<Integer> setIP2 = new HashSet<Integer>();

		setIP1.addAll(IP1.keySet());
		setIP2.addAll(IP2.keySet());

		int P_ij = 0, Q_i = 0, Q_j = 0;

		Set<Integer> common = new HashSet<Integer>(setIP1);

		common.retainAll(setIP2);

		for (int id : common) {
			P_ij += IP1.get(id) * IP2.get(id);
		}

		PIJ[id1][id2] = P_ij;
		PIJ[id2][id1] = P_ij;
		// return P_ij;
	}

	@SuppressWarnings("unused")
	private static void computeQI(ConcurrentHashMap<Integer, Double> IP, int id1) {
		Set<Integer> setIP = new HashSet<Integer>();

		setIP.addAll(IP.keySet());

		int Q_i = 0;

		for (int id : setIP) {
			Q_i += Math.pow(IP.get(id), 2);
		}

		QI[id1] = Q_i;
		// return Q_i;
	}

	private static void updatePIJ(int id1, int id2) throws IOException {
		int delta_PIJ = 0;

		// System.out.println(delta_itemProfiles.get(id1).keySet());

		Set<Integer> common_deltaUI_UJ = new HashSet<Integer>(delta_itemProfiles.get(id1).keySet());
		common_deltaUI_UJ.retainAll(itemProfiles.get(id2).keySet());
		Set<Integer> common_deltaUJ_UI = new HashSet<Integer>(delta_itemProfiles.get(id2).keySet());
		common_deltaUJ_UI.retainAll(itemProfiles.get(id1).keySet());
		Set<Integer> common_deltaUI_deltaUJ = new HashSet<Integer>(delta_itemProfiles.get(id1).keySet());
		common_deltaUI_deltaUJ.retainAll(delta_itemProfiles.get(id2).keySet());

		// Update delta terms

		// update delta_PIJ
		for (int id : common_deltaUI_UJ) {
			delta_PIJ += (delta_itemProfiles.get(id1).get(id)) * (itemProfiles.get(id2).get(id));
		}

		for (int id : common_deltaUJ_UI) {
			delta_PIJ += (delta_itemProfiles.get(id2).get(id)) * (itemProfiles.get(id1).get(id));
		}

		for (int id : common_deltaUI_deltaUJ) {
			delta_PIJ += (delta_itemProfiles.get(id1).get(id)) * (delta_itemProfiles.get(id2).get(id));
		}

		// Update actual terms
		PIJ[id1][id2] = delta_PIJ + Math.pow(Math.E, -2 * alpha) * PIJ[id1][id2];

		PIJ[id2][id1] = PIJ[id1][id2];

	}

	private static void updateQI(int id1) throws IOException {
		double delta_QI = 0;
		// update Q_i
		for (int id : delta_itemProfiles.get(id1).keySet()) {
			delta_QI += Math.pow(delta_itemProfiles.get(id1).get(id), 2);
		}

		QI[id1] = delta_QI + Math.pow(Math.E, -2 * alpha) * QI[id1];
	}

	
	private static void incrementalKNN() {
		boolean DESC = false;
		for (int i : itemProfiles.keySet()) {
			Map<Integer, Double> sims = new HashMap<Integer, Double>();
			for (int j : itemProfiles.keySet()) {
				// Incremental cosine
				if (QI[i] == 0 || QI[j] == 0)
					sims.put(j, 0.0);

				else
					sims.put(j, PIJ[i][j] / (double) (Math.sqrt(QI[i]) * Math.sqrt(QI[j])));
			}

			// Get top-k
			Map<Integer, Double> sortedMapDesc = sortByComparator(sims, DESC);
			ArrayList<Integer> subItems = new ArrayList<Integer>(sortedMapDesc.keySet());
			KNN.put(i, subItems.subList(0, K - 1));
		}
	}
	
	
	@SuppressWarnings("unused")
	private static void nonIncrementalKNN() {
		boolean DESC = false;
		for(int id1: itemProfiles.keySet()){
			Map<Integer,Double> sims=new HashMap<Integer,Double>();
			for(int id2: itemProfiles.keySet()){
				//Non-incremental cosine
				try {
					sims.put(id2, getCosineSimilarity(itemProfiles.get(id1),itemProfiles.get(id2),id1,id2));
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
			Map<Integer,Double> sortedMapDesc = sortByComparator(sims, DESC);
	        ArrayList<Integer> subItems = new ArrayList<Integer>(sortedMapDesc.keySet());
			KNN.put(id1, subItems.subList(0, K-1));
		}
	}
	
	private static LinkedHashMap<Long, ArrayList<Rating>> parseData(String trainPath) {
		
		ArrayList<Integer> timelineUser = new ArrayList<Integer>();
		ArrayList<Integer> timelineItem = new ArrayList<Integer>();
		ArrayList<Double> timelineRating = new ArrayList<Double>();
		ArrayList<Long> timelineTime = new ArrayList<Long>();
		ArrayList<Integer> itemList = new ArrayList<Integer>();
		
		try {
			BufferedReader br = new BufferedReader(new FileReader(trainPath));
			String line;
			while ((line = br.readLine()) != null) {
				Integer uid = Integer.parseInt(line.substring(0, line.indexOf(",")));
				timelineUser.add(uid);
				line = line.substring(line.indexOf(",") + 1);
				Integer rid = Integer.parseInt(line.substring(0, line.indexOf(",")));
				maxItemID = Math.max(maxItemID, rid);
				timelineItem.add(rid);
				if (!itemList.contains(rid)) {
					itemList.add(rid);
				}

				line = line.substring(line.indexOf(",") + 1);
				Double rating = Double.parseDouble(line.substring(0, line.indexOf(",")));
				timelineRating.add(rating);

				line = line.substring(line.indexOf(",") + 1);
				long time = Long.parseLong(line);
				timelineTime.add(time);

			}
			br.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

		System.out.println("Max size: " + maxItemID + "\nNumber of items: " + itemList.size() + "\nNumber of ratings: "
				+ timelineTime.size());
		

		LinkedHashMap<Long, ArrayList<Rating>> data = new LinkedHashMap<Long, ArrayList<Rating>>();

		for (int i = 0; i < timelineTime.size(); i++) {

			long timestamp = timelineTime.get(i);

			if (!data.containsKey(timestamp)) {
				data.put(timestamp, new ArrayList<Rating>());
			}
			data.get(timestamp).add(new Rating(timelineUser.get(i), timelineItem.get(i), timelineRating.get(i)));
		}
		
		return data;
	}

}

