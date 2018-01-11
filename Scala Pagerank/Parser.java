/**
 * 
 */
package utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import mapper.ParserMapper;

/**
 * @author ajay
 *
 */
public class Parser {
	private static Long pageCnt = 0L;
	private static Configuration pageRankConf;
	
	public static Long getPageCnt() {
		return pageCnt;
	}

	public static void setPageCnt(Long pageCnt) {
		Parser.pageCnt = pageCnt;
	}

	public static void main(String args[]) {
		process(args);
	}
	
	public static void process(String args[]) {
		pageRankConf = new Configuration();
		String[] otherArgs = null;
		try {
			otherArgs = new GenericOptionsParser(pageRankConf, args).getRemainingArgs();

			if (otherArgs.length < 2) {
				System.out.println("Please specify input path and output path");
				System.exit(0);
			}

			// pre-processing of the bz2 files
			preProcessBZ2(pageRankConf, otherArgs[0]);
			System.out.println("PRE PROCESSING OF BZ2 IS COMPLETE");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/*
	 * Method to parse BZ2 input files
	 * 
	 * @param conf
	 * 
	 * @param input
	 * 
	 * @throws Exception
	 */
	public static void preProcessBZ2(Configuration conf, String input) throws Exception {

		Job job = Job.getInstance(conf, "parse input");

		job.setMapperClass(ParserMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(NullWritable.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		job.setNumReduceTasks(1);

		// configure output directory for the job
		StringBuilder op = new StringBuilder();
		op.append(input);
		op.append("/parser_output");

		FileInputFormat.addInputPath(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path(op.toString()));

		boolean ok = job.waitForCompletion(true);
		if (!ok) {
			throw new Exception("Job failed");
		}

		// read the values from counter, to be used as inputs to JOB-2

		Counters counters = job.getCounters();
		pageCnt = counters.findCounter(PageRankCounter.PAGE_COUNTER).getValue();

	}

}
