package reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import model.Record;

/**
 * This reducer to reduce and emit top K records (i.e. 100)
 * 
 * @author ajay
 */

public class TopKReducer extends Reducer<DoubleWritable, Text, Text, DoubleWritable> {
	
	private Integer top_k;
	private List<Record> topRecords;
	
	public void setup(Context ctx){
		topRecords = new ArrayList<Record>();
		// read the size of top records to be emitted
		// in this case, top_k = 100
		top_k = ctx.getConfiguration().getInt("TOP_K",-1);
	}
	
	
	public void reduce(DoubleWritable key, Iterable<Text> nodes, Context ctx) throws InterruptedException, IOException{
	
		for(Text node : nodes){
			if(topRecords.size() < top_k){
				topRecords.add(new Record(node.toString(), key.get()));
			}
		}
	}
	
	public void cleanup(Context ctx) throws IOException, InterruptedException{
		// emit TOP K records
		for(Record record : topRecords){
			ctx.write(new Text(record.getNode()), new DoubleWritable(record.getPageRank()));
		}
	}
}
