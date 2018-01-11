package mapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * This mapper reads the records from the final iteration, 
 * and sorts them in the decreasing order of page rank scores
 * and emits only the top K, by performing in-mapper combine 
 * so that data being sent to reducer is minimum
 * 
 * @author ajay
 */

public class TopKMapper extends Mapper<Object, Text, DoubleWritable, Text> {
	
	private Integer top_k;
	private Map<String, Double> resultMap;
	public void setup(Context ctx){
		// read the size of top records to be emitted
		// in this case, top_k = 100
		top_k = ctx.getConfiguration().getInt("TOP_K",-1);
		resultMap = new HashMap<String, Double>();
	}
	
	public void map(Object _k, Text line, Context ctx) throws IOException, InterruptedException {
		if(line.toString().trim().isEmpty() || (!line.toString().contains("#")))
			return;
		
		// parse the record, and add the node and page rank score to a map
		
		String[] tokens = line.toString().split("#");
		String node = tokens[0]; // node
		Double pageRank = Double.parseDouble(tokens[2]); // its page rank value
		resultMap.put(node, pageRank);
	}
	
	public void cleanup(Context ctx) throws IOException, InterruptedException{
		
		// sort the records by page rank value in descending order
		List<Entry<String, Double>> recordList = new ArrayList<Entry<String, Double>>(resultMap.entrySet()); 
		Collections.sort(recordList, new Comparator<Entry<String, Double>>() {

			public int compare(Entry<String, Double> o1,
					Entry<String, Double> o2) {
				return o2.getValue().compareTo(o1.getValue());
			}
		});
		
		// select only top K records
		List<Entry<String, Double>> top_k_records = recordList.subList(0, top_k);
		
		// emit top K records
		for(Entry<String, Double> record : top_k_records){
			ctx.write(new DoubleWritable(record.getValue()), new Text(record.getKey()));
		}
	}
}