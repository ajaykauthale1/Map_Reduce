/**
 * 
 */
package seq;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import model.StationModel;
import util.LoaderRoutine;
import util.ProcessUtility;

/**
 * @author Ajay
 *
 *         Sequential version that calculates the average of the TMAX
 *         temperatures by station Id.
 */
public class SeqFib {

	/**
	 * Method to calculate TMAX avg sequentially
	 * 
	 */
	public static void SeqFibTMAX() {
		// load file and convert it into simple lines
		List<String> lines = LoaderRoutine.loadFile("1912.csv.gz");

		PrintStream out = null;
		try {
			out = new PrintStream(new FileOutputStream(LoaderRoutine.ROOT_DIR + "\\output\\seqfib.txt"));
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
		System.setOut(out);

		// map for avgTMAX temperatures by station
		Map<String, Double> avgTMAX = new LinkedHashMap<String, Double>();
		// map for station model (which contains total temperature and count)
		Map<String, StationModel> stations = new LinkedHashMap<String, StationModel>();
		List<Long> runningTimes = new ArrayList<Long>();
		// execute 10 times
		for (int i = 0; i < 10; i++) {
			long startingTime = System.currentTimeMillis();
			avgTMAX = processTMAX(lines, stations);
			long endTime = System.currentTimeMillis();
			runningTimes.add((endTime - startingTime));
		}

		long totalTime = 0;
		for (long time : runningTimes) {
			totalTime += time;
		}

		System.out.println("----------------------------------------------------------------------------------------");
		System.out.println("										RESULTS											");
		System.out.println("----------------------------------------------------------------------------------------");
		System.out.println("Minimum Running Time in milisecond:" + Collections.min(runningTimes));
		System.out.println("Maximum Running Time in milisecond:" + Collections.max(runningTimes));
		System.out.println("Average Running Time in milisecond:" + totalTime / runningTimes.size());
		System.out.println("----------------------------------------------------------------------------------------");
		System.out.println("Station Id         AvgTMAX");
		System.out.println("----------------------------------------------------------------------------------------");
		for (String key : avgTMAX.keySet()) {
			System.out.println(key + " " + avgTMAX.get(key));
		}
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		SeqFib.SeqFibTMAX();
	}

	/**
	 * Process raw data into meaningful one 
	 * 
	 * @param unprocessedData unprocessed temperature data
	 * @return map of avg TMAX by station id
	 */
	public static Map<String, Double> processTMAX(List<String> unprocessedData, Map<String, StationModel> stationMap) {
		for (String line : unprocessedData) {
			String[] arr = line.split(",");
			if (arr[2] != null && arr[2].equals("TMAX")) {
				String stationName = arr[0];
				Double temp = Double.parseDouble(arr[3]);
				StationModel newStation;
				// update TMAX if station already there
				if (stationMap.containsKey(stationName)) {
					newStation = stationMap.get(stationName);
					// run fibonacci to slow down
					ProcessUtility.fibonacciRun(17);
					newStation.addNextTemperature(temp);
					stationMap.put(stationName, newStation);
				} // else add new station with TMAX
				else {
					newStation = new StationModel();
					newStation.addNextTemperature(temp);
					stationMap.put(stationName, newStation);
				}
			}
		}

		// calculate and return Avg TMAX for each station
		return ProcessUtility.getAvgTmaxByStation(stationMap);
	}
}
