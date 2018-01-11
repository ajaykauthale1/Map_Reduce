/**
 * 
 */
package comparator;

/**
 * comparator sorts records emitted by Mapper in descending order
 * 
 * @author ajay
 *
 */
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class PageRankKeyComparator extends WritableComparator {

	public PageRankKeyComparator(){
		super(DoubleWritable.class, true);
	}
	
	@Override
	public int compare(WritableComparable obj1, WritableComparable obj2){
		
		DoubleWritable pageRank1 = (DoubleWritable) obj1;
		DoubleWritable pageRank2 = (DoubleWritable) obj2;
		// sort in descending order of page-rank values
		return pageRank2.compareTo(pageRank1);
	}
	
}
