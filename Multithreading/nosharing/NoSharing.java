package nosharing;

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
 *         No Sharing version that calculates the average of the TMAX
 *         temperatures by station Id.
 */
public class NoSharing {
	// map for avgTMAX temperatures by station
	public static Map<String, Double> avgTMAX = new LinkedHashMap<String, Double>();
	// map for station model (which contains total temperature and count)
	public static Map<String, StationModel> stationMap = new LinkedHashMap<String, StationModel>();

	/**
	 * Method to calculate TMAX avg using no lock
	 * 
	 */
	public static void noSharingTMAX() {
		// load file and convert it into simple lines
		List<String> lines = LoaderRoutine.loadFile("1912.csv.gz");

		PrintStream out = null;
		try {
			out = new PrintStream(new FileOutputStream(LoaderRoutine.ROOT_DIR + "\\output\\nosharing.txt"));
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
			processTMAX(lines, stations);
			long endTime = System.currentTimeMillis();
			runningTimes.add((endTime - startingTime));
		}

		long totalTime = 0;
		for (long time : runningTimes) {
			totalTime += time;
		}

		// calculate and return Avg TMAX for each station
		avgTMAX = ProcessUtility.getAvgTmaxByStation(stationMap);

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
		NoSharing.noSharingTMAX();
	}

	/**
	 * Process raw data into meaningful one (thread partitions and running
	 * referred online)
	 * 
	 * @param unprocessedData
	 *            unprocessed temperature data
	 */
	public static void processTMAX(List<String> unprocessedData, Map<String, StationModel> stationMap) {
		// System has 2 actual and 4 logical processors, so divide the list into
		// 4 part and process each part by
		// separate thread simultaneously
		int size = unprocessedData.size();
		List<String> data1 = unprocessedData.subList(0, size / 4);
		List<String> data2 = unprocessedData.subList(size / 4, size / 2);
		List<String> data3 = unprocessedData.subList(size / 2, (3 * size / 4));
		List<String> data4 = unprocessedData.subList((3 * size / 4), size);

		// create 4 threads and pass the data share
		MyThread thread1 = new MyThread(data1, "Thread1");
		MyThread thread2 = new MyThread(data2, "Thread2");
		MyThread thread3 = new MyThread(data3, "Thread3");
		MyThread thread4 = new MyThread(data4, "Thread4");

		// start each thread
		thread1.start();
		thread2.start();
		thread3.start();
		thread4.start();

		try {
			thread1.join();
			thread2.join();
			thread3.join();
			thread4.join();

			// merge stations maps into one for all threads
			mergeStationMaps(thread1.getStationMap());
			mergeStationMaps(thread2.getStationMap());
			mergeStationMaps(thread3.getStationMap());
			mergeStationMaps(thread4.getStationMap());
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Method to merge station map from each thread into single map
	 * 
	 * @param temperatureData
	 */
	public static void mergeStationMaps(Map<String, StationModel> threadStationMap) {
		// iterate over the thread station map and add it to actual map
		for (String key : threadStationMap.keySet()) {
			// if station already present then update it with new count and total temp
			if (stationMap.containsKey(key)) {
				StationModel station = threadStationMap.get(key);
				Double currentTemp = station.getCurrentTotalTemp();
				int currentCnt = station.getTempCount();
				StationModel actualStation = stationMap.get(key);
				actualStation.setCurrentTotalTemp(currentTemp + station.getCurrentTotalTemp());
				actualStation.setTempCount(currentCnt + station.getTempCount());
				stationMap.put(key, actualStation);
			} else {
				// if station not present the add it to the map
				StationModel station = threadStationMap.get(key);
				stationMap.put(key, station);
			}
		}
	}

	/**
	 * Inner class for creating threads
	 * 
	 * @author Ajay
	 *
	 */
	static class MyThread extends Thread {
		// Below map is separate for each thread, hence no sharing
		// map for station model (which contains total temperature and count)
		public Map<String, StationModel> stationMap = new LinkedHashMap<String, StationModel>();

		// Unprocessed temperature data
		private final List<String> unprocessedData;
		private String threadName;

		public MyThread(List<String> unprocessedData, String name) {
			this.unprocessedData = unprocessedData;
			this.threadName = name;
			this.stationMap = new LinkedHashMap<String, StationModel>();
		}

		/**
		 * Method to get access of station map outside of the thread
		 * 
		 * @return stationmap
		 */
		public Map<String, StationModel> getStationMap() {
			return this.stationMap;
		}

		public void run() {
			// parse the data by station Id
			for (String line : unprocessedData) {
				String[] arr = line.split(",");
				if (arr[2] != null && arr[2].equals("TMAX")) {
					String stationName = arr[0];
					Double temp = Double.parseDouble(arr[3]);
					StationModel newStation;
					// update TMAX if station already there
					if (this.stationMap.containsKey(stationName)) {
						newStation = this.stationMap.get(stationName);
						newStation.addNextTemperature(temp);
						this.stationMap.put(stationName, newStation);
					} // else add new station with TMAX
					else {
						newStation = new StationModel();
						newStation.addNextTemperature(temp);
						this.stationMap.put(stationName, newStation);
					}
				}
			}

		}
	}
}
