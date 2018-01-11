package comparator;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import model.StationKey;

/**
 * Comparator to sort stations by id and year together
 * 
 * @author ajay
 *
 */
public class StationKeyComparator extends WritableComparator {

	/**
	 * Constructor
	 */
	protected StationKeyComparator() {
		super(StationKey.class, true);
	}

	@Override
	public int compare(WritableComparable obj1, WritableComparable obj2) {

		StationKey k1 = (StationKey) obj1;
		StationKey k2 = (StationKey) obj2;

		if (k1.getId().compareTo(k2.getId()) == 0) {
			return k1.getYear().compareTo(k2.getYear());
		} else {
			return k1.getId().compareTo(k2.getId());
		}
	}
}
