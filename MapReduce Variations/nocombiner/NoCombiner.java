/**
 * Program with no combiner or custom partitioner
 */
package nocombiner.nocombiner;

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
 * No Combiner class
 * 
 * @author ajay
 *
 */
public class NoCombiner {

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
			job.setJarByClass(NoCombiner.class);
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

			// else extract temperature and its type, and write new station in
			// to the context
			String[] arr = line.split(",");
			String station = arr[0];
			boolean isTMAX = false;
			if (arr[2] != null && arr[2].equals("TMAX")) {
				isTMAX = true;
			}

			Double tmp = Double.parseDouble(arr[3]);

			StationModel newStation = new StationModel(isTMAX, tmp);

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
		int tMAXCnt = 0, tMINCnt = 0;

		// iterate over values and do the reducing
		for (StationModel station : values) {
			if (station.isTMAX()) {
				totalTMAX += station.getTmp();
				tMAXCnt++;
			} else {
				totalTMIN += station.getTmp();
				tMINCnt++;
			}
		}

		// create record to write
		StringBuilder station = new StringBuilder();
		station.append(key.toString()).append(", ");
		station.append(((tMINCnt == 0) ? "No TMIN Records" : (totalTMIN / tMINCnt))).append(", ");
		station.append(((tMAXCnt == 0) ? "No TMAX Records" : (totalTMAX / tMAXCnt)));

		Text text = new Text();
		text.set(station.toString());

		context.write(text, nullWritable);
	}
}
