/**
 * 
 */
package comparator;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import model.StationKey;

/**
 * Comparator to sort stations by id
 * 
 * @author ajay
 *
 */
public class StationComparator extends WritableComparator {

	/**
	 * Constructor
	 */
	protected StationComparator() {
		super(StationKey.class, true);
	}

	@Override
	public int compare(WritableComparable obj1, WritableComparable obj2) {
		StationKey k1 = (StationKey) obj1;
		StationKey k2 = (StationKey) obj2;
		return k1.compareTo(k2);
	}
}
