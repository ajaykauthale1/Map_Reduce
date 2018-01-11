/**
 * 
 */
package mapper;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import model.Node;
import utils.PageRankCounter;

/**
 * Mapper class for page ranking
 * 
 * @author ajay
 *
 */
public class PageRankMapper extends Mapper<Object, Text, Text, Node> {
	// Total page count of last iteration
	private Long cnt; 
	// To store current iteration
	private Integer currentItr;
	// Constant to convert the dangling node score to long
	private final Long MULTIPLIER = 1000000000000l;  
	
	@Override
	public void setup(Context ctx){
		// read fields that are set in configuration
		currentItr = ctx.getConfiguration().getInt("ITERATION", -1);
		cnt = (long) ctx.getConfiguration().getLong("PAGE_COUNT", -1);
	}
	
	@Override
	public void map(Object _k, Text line, Context ctx) throws IOException, InterruptedException {
		if(line.toString().trim().isEmpty() || (!line.toString().contains("#")))
			return;
		
		// RECORD FORMAT: 
		// Z#A~B~C#PR_VALUE, where A,B,C are outlinks(adjacency list) of NODE Z
		
		// PARSE records according to the format, to process
		String[] tokens = line.toString().split("#");
		String node = tokens[0];
		Double pageRankValue = 0.0;
		
		// if this is first iteration, default page rank values will be 1/N
		// else read the existing page rank value from record and distribute amongst the outlinks of the node
		if(currentItr == 0){
			pageRankValue = Double.valueOf(1.0/cnt);	
		}else{
			pageRankValue = Double.parseDouble(tokens[2]);
		}
		
		// if current node is dangling node, accumulate its score in the global counter
		if(tokens[1].trim().length()==0){
			ctx.getCounter(PageRankCounter.DANGLING_NODE_SCORE).increment((long) (pageRankValue*MULTIPLIER));
			ctx.write(new Text(node), new Node(false, tokens[1]));
		}// if current node is not a dangling node, distribute its page rank score amongst its outlinks
		else{
			String[] outlinks = tokens[1].split("~");
			for(String outlink : outlinks){
				ctx.write(new Text(outlink), new Node(true, pageRankValue, outlinks.length));
			}
			
			// emit node's adjacency list for reducer to output in the same format for mapper of next iteration to read, for consistency 
			ctx.write(new Text(node), new Node(false, tokens[1].trim()));
		}
		
	}
	

}