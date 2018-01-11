/**
 * Program with in mapper combining
 */
package inmappercomb.inmappercomb;

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

import model.StationModel;

/**
 * In mapper combiner class
 * 
 * @author ajay
 *
 */
public class InMapperComb {

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
			job.setJarByClass(InMapperComb.class);
			job.setMapperClass(StationMapper.class);
			job.setReducerClass(StationTemperatureReducer.class);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(StationModel.class);
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
class StationMapper extends Mapper<Object, Text, Text, StationModel> {
	// map used for in mapper combining
	Map<String, StationModel> stationMap;

	// instantiate the map using hashmap for in mapper combining
	public void setup(Context context) {
		stationMap = new HashMap<String, StationModel>();
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
			StationModel newStation = null;

			// if station is new then create record in the map
			if (!stationMap.containsKey(station)) {
				stationMap.put(station, new StationModel(0.0, 0.0, 0, 0));
			}

			// update the in mapper map with appropriate TMAX and TMIN
			if (arr[2] != null && arr[2].equals("TMAX")) {
				newStation = stationMap.get(station);
				newStation.setTMAX(newStation.getTMAX() + Double.parseDouble(arr[3]));
				newStation.setTMAXCnt(newStation.getTMAXCnt() + 1);
				stationMap.put(station, newStation);
			} else {
				newStation = stationMap.get(station);
				newStation.setTMIN(newStation.getTMIN() + Double.parseDouble(arr[3]));
				newStation.setTMINCnt(newStation.getTMINCnt() + 1);
				stationMap.put(station, newStation);
			}

		}
	}

	// clean up the map used for in mapper combining by emiting records
	public void cleanup(Context context) throws IOException, InterruptedException {
		for (Entry<String, StationModel> sEntry : stationMap.entrySet()) {
			context.write(new Text(sEntry.getKey()), sEntry.getValue());
		}
	}
}

/**
 * Reducer class for reducing the temperatures by station id
 * 
 * @author ajay
 *
 */
class StationTemperatureReducer extends Reducer<Text, StationModel, Text, NullWritable> {
	NullWritable nullWritable = NullWritable.get();

	@Override
	public void reduce(Text key, Iterable<StationModel> values, Context context)
			throws IOException, InterruptedException {
		// for reducing, add TMIN and TMAX together for each station
		Double totalTMAX = 0.0, totalTMIN = 0.0;
		int TMAXCnt = 0, TMINCnt = 0;

		// iterate over values and do the reducing
		for (StationModel station : values) {
			totalTMAX += station.getTMAX();
			TMAXCnt += station.getTMAXCnt();
			totalTMIN += station.getTMIN();
			TMINCnt += station.getTMINCnt();
		}

		// create record to write
		StringBuilder station = new StringBuilder();
		station.append(key.toString()).append(", ");
		station.append(((TMINCnt == 0) ? "No TMIN Records" : (totalTMIN / TMINCnt))).append(", ");
		station.append(((TMAXCnt == 0) ? "No TMAX Records" : (totalTMAX / TMAXCnt)));

		Text text = new Text();
		text.set(station.toString());

		context.write(text, nullWritable);
	}
}

