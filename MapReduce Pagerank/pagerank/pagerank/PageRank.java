/**
 * 
 */
package pagerank.pagerank;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import comparator.PageRankKeyComparator;
import mapper.PageRankMapper;
import mapper.ParserMapper;
import mapper.TopKMapper;
import model.Node;
import partitioner.PageRankPartitioner;
import reducer.PageRankReducer;
import reducer.TopKReducer;
import utils.PageRankCounter;

/**
 * @author ajay
 *
 */
public class PageRank {
	private static Long pageCnt = 0L;
	private static final int MAX_ITR = 10;
	private static Path input;
	private static Long danglingNodeScore = 0L;
	private static Configuration pageRankConf;
	private static final int TOP_K = 100;

	/**
	 * Main method to start page ranking
	 * 
	 * @param args
	 */
	public static void main(String[] args) {

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

			// iterate 10 times
			for (int i = 0; i < MAX_ITR; i++) {
				System.out.println("ITR: " + i);
				getPageRank(pageRankConf, i, otherArgs[0]);
			}

			// get top 100 results
			topKResults(pageRankConf, otherArgs[0], otherArgs[1]);

			System.out.println("PAGE RANK COMPLETED");
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	/**
	 * Method for getting top 100 results in descending order
	 * 
	 * @param conf
	 * @param input
	 * @param output
	 * @throws Exception
	 */
	private static void topKResults(Configuration conf, String input, String output) throws Exception {

		// configure mappers and reducers
		conf.set("TOP_K", TOP_K + "");

		Job job = Job.getInstance(conf, "top k results");
		job.setJarByClass(PageRank.class);

		job.setMapperClass(TopKMapper.class);
		job.setReducerClass(TopKReducer.class);
		job.setSortComparatorClass(PageRankKeyComparator.class);
		job.setPartitionerClass(PageRankPartitioner.class);

		job.setMapOutputKeyClass(DoubleWritable.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);

		StringBuilder ip = new StringBuilder();
		ip.append(input);
		ip.append("/page_rank_output");
		ip.append("/PR-" + (MAX_ITR - 1));

		FileInputFormat.addInputPath(job, new Path(ip.toString()));
		FileOutputFormat.setOutputPath(job, new Path(output));

		boolean isComplete = job.waitForCompletion(true);
		if (!isComplete) {
			throw new Exception("Job failed");
		}

	}

	/**
	 * Method to parse BZ2 input files
	 * 
	 * @param conf
	 * @param input
	 * @throws Exception
	 */
	public static void preProcessBZ2(Configuration conf, String input) throws Exception {

		Job job = Job.getInstance(conf, "parse input");
		job.setJarByClass(PageRank.class);

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

	/**
	 * Method to calculate the page rank 
	 * 
	 * @param conf
	 * @param iteration
	 * @param input
	 * @throws Exception
	 */
	public static void getPageRank(Configuration conf, int iteration, String input) throws Exception {

		// set inputs to be used in mappers and reducers
		conf.setInt("ITERATION", iteration);
		conf.setLong("PAGE_COUNT", pageCnt);
		conf.setLong("DANGLING_NODE_SCORE", (iteration == 0 ? 0 : danglingNodeScore));

		Job job = Job.getInstance(conf, "compute page rank");
		job.setJarByClass(PageRank.class);

		job.setMapperClass(PageRankMapper.class);
		job.setReducerClass(PageRankReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Node.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);

		StringBuilder ip = new StringBuilder();
		StringBuilder output = new StringBuilder();

		// for first iteration, output of parser job is the input
		if (iteration == 0) {
			ip.append(input);
			ip.append("/parser_output");
		} // for other iteration, output of previous iteration is the input
		else {
			ip.append(input);
			ip.append("/page_rank_output");
			ip.append("/PR-" + (iteration - 1));
		}

		// configure output directory for the current iteration
		output.append(input);
		output.append("/page_rank_output");
		output.append("/PR-" + iteration);

		FileInputFormat.addInputPath(job, new Path(ip.toString()));
		FileOutputFormat.setOutputPath(job, new Path(output.toString()));

		boolean ok = job.waitForCompletion(true);
		if (!ok) {
			throw new Exception("Job failed");
		}

		// read the values from counter, to be used as inputs to next iteration
		Counters counters = job.getCounters();
		pageCnt = counters.findCounter(PageRankCounter.PAGE_COUNTER).getValue();
		danglingNodeScore = counters.findCounter(PageRankCounter.DANGLING_NODE_SCORE).getValue();

	}
}
