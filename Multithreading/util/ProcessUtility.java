package util;

import java.util.LinkedHashMap;
import java.util.Map;

import model.StationModel;

/**
 * Helper class for simplifying the calculation efforts
 * 
 * @author Ajay
 */
public class ProcessUtility {

	/**
	 * Method to get the TMX average for each station
	 * 
	 * @param tempMap
	 *            map containing the station model
	 * @return the map containing avg TMAX for each station
	 */
	public static Map<String, Double> getAvgTmaxByStation(Map<String, StationModel> tempMap) {
		Map<String, Double> avgTemp = new LinkedHashMap<String, Double>();

		// Iterate over each station and calculate average TMAX
		for (String key : tempMap.keySet()) {
			Double avgTmax = tempMap.get(key).calculateAvgTemp();
			avgTemp.put(key, avgTmax);
		}

		return avgTemp;
	}

	/**
	 * Method to run fibonacci while calculating TMAX
	 * 
	 * @param n input for fibonacci run which is 17
	 * @return return fibonacci result for input
	 */
	public static int fibonacciRun(int n) {
		if (n <= 1)
			return n;
		else
			return (fibonacciRun(n - 1) + fibonacciRun(n - 2));
	}
}
