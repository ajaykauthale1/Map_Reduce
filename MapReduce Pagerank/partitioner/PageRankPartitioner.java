/**
 * 
 */
package partitioner;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * Partitioner to partition and send data to single reducer
 * 
 * @author ajay
 **/

public class PageRankPartitioner extends Partitioner<DoubleWritable, Text> implements Configurable {

	public Configuration getConf() {
		return null;
	}

	public void setConf(Configuration arg0) {
		
	}

	public int getPartition(DoubleWritable arg0, Text arg1, int arg2) {
		return 0;
	}

}