/**
 * 
 */
package secondarysort.secondarysort;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import comparator.StationComparator;
import comparator.StationKeyComparator;
import model.StationKey;
import model.StationModel;

/**
 * @author ajay
 *
 */
public class SecondarySort {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		Configuration conf = new Configuration();

		try {
			String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
			if (otherArgs.length < 2) {
				System.err.println("Usage: hadoop jar This.jar <in> [<in>...] <out>");
				System.exit(2);
			}
			Job job;
			job = new Job(conf, "job1");
			job.setJarByClass(SecondarySort.class);
			job.setMapperClass(StationMapper.class);
			job.setReducerClass(StationTemperatureReducer.class);
			// set key comparator
			job.setSortComparatorClass(StationKeyComparator.class);
			// set group comparator
			job.setGroupingComparatorClass(StationComparator.class);
			job.setMapOutputValueClass(StationModel.class);
			job.setMapOutputKeyClass(StationKey.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(NullWritable.class);
			for (int i = 0; i < otherArgs.length - 1; ++i) {
				FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
			}
			FileOutputFormat.setOutputPath(job, new Path(otherArgs[otherArgs.length - 1]));
			System.exit(job.waitForCompletion(true) ? 0 : 1);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}

/**
 * Mapper class for mapping stations
 * 
 * @author ajay
 *
 */
class StationMapper extends Mapper<Object, Text, StationKey, StationModel> {
	// map used for in mapper combining
	Map<StationKey, StationModel> stationMap;

	// instantiate the map using hashmap for in mapper combining
	public void setup(Context context) {
		stationMap = new HashMap<StationKey, StationModel>();
	}

	@Override
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		// get all lines from the value
		String[] lines = value != null ? value.toString().split("\n") : new String[] {};

		// iterate over each line and find TMAX and TMIN record
		for (String line : lines) {
			// if record does not contain TMAX and TMIN, then continue
			if (line != null && (!line.contains("TMAX") && !line.contains("TMIN"))) {
				continue;
			}

			// else extract temperature and perform in mapper combining
			String[] arr = line.split(",");
			String station = arr[0];
			// get year
			Integer year = Integer.parseInt(arr[1].trim().substring(0, 4));
			StationKey stationKey = new StationKey(station, year);
			StationModel newStation = null;

			// if station is new then create record in the map
			if (!stationMap.containsKey(stationKey)) {
				stationMap.put(stationKey, new StationModel(0.0, 0.0, 0, 0));
			}

			// update the in mapper map with appropriate TMAX and TMIN
			if (arr[2] != null && arr[2].equals("TMAX")) {
				newStation = stationMap.get(stationKey);
				newStation.setTMAX(newStation.getTMAX() + (arr[3] != null ? Double.parseDouble(arr[3]) : 0.0));
				newStation.setTMAXCnt(newStation.getTMAXCnt() + 1);
				stationMap.put(stationKey, newStation);
			} else {
				newStation = stationMap.get(stationKey);
				newStation.setTMIN(newStation.getTMIN() + (arr[3] != null ? Double.parseDouble(arr[3]) : 0.0));
				newStation.setTMINCnt(newStation.getTMINCnt() + 1);
				stationMap.put(stationKey, newStation);
			}

		}
	}

	// clean up the map used for in mapper combining by emiting records
	public void cleanup(Context context) throws IOException, InterruptedException {
		for (Entry<StationKey, StationModel> sEntry : stationMap.entrySet()) {
			context.write(sEntry.getKey(), sEntry.getValue());
		}
	}
}

/**
 * Reducer class for reducing the temperatures by station id
 * 
 * @author ajay
 *
 */
class StationTemperatureReducer extends Reducer<StationKey, StationModel, Text, NullWritable> {
	NullWritable nullWritable = NullWritable.get();

	@Override
	public void reduce(StationKey key, Iterable<StationModel> values, Context context)
			throws IOException, InterruptedException {
		// for reducing, add TMIN and TMAX together for each station
		Double totalTMAX = 0.0, totalTMIN = 0.0;
		int TMAXCnt = 0, TMINCnt = 0;

		// get years from key
		int currentYear = key.getYear(), prevYear = key.getYear();

		// accumulate station's temperature data output
		StringBuilder result = new StringBuilder();
		result.append(key.getId());
		result.append(",[");
		String delimiter = "";

		for (StationModel station : values) {
			prevYear = currentYear;
			currentYear = key.getYear();

			// if current year and previous year is not matched for same
			// station, then write previous record to context
			if (prevYear != currentYear) {
				result.append(delimiter);
				delimiter = ",";
				result.append("(").append(prevYear).append(" ,");
				result.append(((TMINCnt == 0) ? "No TMIN Records" : (totalTMIN / TMINCnt))).append(", ");
				result.append(((TMAXCnt == 0) ? "No TMAX Records" : (totalTMAX / TMAXCnt))).append(")");

				totalTMAX = 0.0;
				totalTMIN = 0.0;
				TMAXCnt = 0;
				TMINCnt = 0;
			}
			
			// add TMIN,TMAX and their counts
			totalTMAX += station.getTMAX();
			TMAXCnt += station.getTMAXCnt();
			totalTMIN += station.getTMIN();
			TMINCnt += station.getTMINCnt();
		}

		// for last year, if current year and previous year is matched for same
		// station, then append the result
		if (prevYear == currentYear) {
			result.append(delimiter);
			delimiter = ",";
			result.append("(").append(prevYear).append(" ,");
			result.append(((TMINCnt == 0) ? "No TMIN Records" : (totalTMIN / TMINCnt))).append(", ");
			result.append(((TMAXCnt == 0) ? "No TMAX Records" : (totalTMAX / TMAXCnt))).append(")");
		}

		result.append("]");
		context.write(new Text(result.toString()), nullWritable);
	}

}