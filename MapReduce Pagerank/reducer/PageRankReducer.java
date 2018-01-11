/**
 * 
 */
package reducer;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import model.Node;
import utils.PageRankCounter;

/**
 * PageRankReducer for reducing page rank values
 * 
 * @author ajay
 **/

public class PageRankReducer extends Reducer<Text, Node, Text, NullWritable> {
	
	// Node list
	private Set<String> nodeList;
	// Score for danging node
	private Double danglingNodeScore;
	// Constant for ALPHA
	private final Double ALPHA = 0.15;
	// Counter for pages
	private Long pageCount;
	// TO CONVERT previous iteration's dangling node scores back from long to double
	private final Long DANGLING_NODE_SCORE_MULTIPLIER = 1000000000000l; 
	private NullWritable nw = NullWritable.get();
	
	public void setup(Context ctx){
		nodeList = new HashSet<String>();
		pageCount = ctx.getConfiguration().getLong("PAGE_COUNT", -1);
		danglingNodeScore = Double.valueOf(ctx.getConfiguration().getLong("DANGLING_NODE_SCORE", -1));
		danglingNodeScore = danglingNodeScore/DANGLING_NODE_SCORE_MULTIPLIER;
	}
	
	public void reduce(Text key, Iterable<Node> vals, Context ctx) throws InterruptedException, IOException{
		String node = key.toString();
		String adjacencyList = "";
		Double prSummation = 0.0;
		
		// if record is of adjacency list, store the adjacency list for the node
		// if record has page rank value and outlink count of inlink of node, accumlate the values
		for(Node nData : vals){
			if(nData.getIsPageRank()){
				if(nData.getOutLinkCount()!=0)
					prSummation+=(nData.getPageRankValue()/nData.getOutLinkCount());	
			}else{
				adjacencyList = nData.getAdjacencyList().toString();
			}
		}
		// distribute the dangling node score of previous iteration amongst all the nodes in the current iteration
		Double danglingScoreDistribution = (danglingNodeScore/pageCount);
		
		// compute page rank score of the node for the current iteration
		Double pagerRankScore = (ALPHA/pageCount)+((1-ALPHA) * (danglingScoreDistribution + prSummation)); 
		
		// track total nodes in current iteration
		nodeList.add(node);
		
		// compose data in the required format and emit
		StringBuilder output = new StringBuilder();
		output.append(node);
		output.append("#");
		output.append(adjacencyList);
		output.append("#");
		output.append(pagerRankScore);
		
		ctx.write(new Text(output.toString()), nw);
		
	}
	
	public void cleanup(Context ctx){
		// set total nodes in the data set computed, in the counter to be used by next iteration
		ctx.getCounter(PageRankCounter.PAGE_COUNTER).setValue(nodeList.size());
	}
}