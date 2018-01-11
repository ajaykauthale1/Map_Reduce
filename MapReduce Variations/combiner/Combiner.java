/**
 * Program with combiner or custom partitioner
 */
package combiner.combiner;

import java.io.IOException;

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
 * Combiner class
 * 
 * @author ajay
 *
 */
public class Combiner {

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
			job.setJarByClass(Combiner.class);
			job.setMapperClass(StationMapper.class);
			job.setReducerClass(StationTemperatureReducer.class);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(StationModel.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(NullWritable.class);
			job.setCombinerClass(TemperatureCombiner.class);
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
			if (arr[2] != null && arr[2].equals("TMAX")) {
				newStation = new StationModel(Double.parseDouble(arr[3]), 0.0, 1, 0);
			} else {
				newStation = new StationModel(0.0, Double.parseDouble(arr[3]), 0, 1);
			}

			// emit new station to context
			context.write(new Text(station), newStation);
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

class TemperatureCombiner extends
Reducer<Text, StationModel, Text, StationModel> {

	public void reduce(Text key, Iterable<StationModel> values,
			Context context) throws IOException, InterruptedException {
		// combine TMAX and TMIN for the particular station id
		Double totalTMAX = 0.0, totoalTMIN = 0.0;
		Integer tMINCnt = 0, tMAXCount = 0;
		// for all record count will be 1 since mapper is emiting same
		for (StationModel station : values) {
			if (station.getTMAXCnt() == 1) {
				tMAXCount += 1;
				totalTMAX += station.getTMAX();
			} else {
				tMINCnt += 1;
				totoalTMIN += station.getTMIN();
			}
		}
		
		// emit the combined record
		context.write(new Text(key), new StationModel(totalTMAX, totoalTMIN,
				tMAXCount, tMINCnt));

	}
}