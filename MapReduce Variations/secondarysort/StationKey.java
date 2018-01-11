/**
 * 
 */
package model;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

/**
 * Class used for secondary sort composite key
 * 
 * @author ajay
 *
 */
public class StationKey implements WritableComparable<StationKey>, Writable {

	// instance variable for station id
	private String id;
	// instance variable for year
	private Integer year;

	/**
	 * 
	 */
	public StationKey() {
		super();
	}

	/**
	 * @param id
	 * @param year
	 */
	public StationKey(String id, Integer year) {
		super();
		this.id = id;
		this.year = year;
	}

	/**
	 * @return the id
	 */
	public String getId() {
		return id;
	}

	/**
	 * @param id
	 *            the id to set
	 */
	public void setId(String id) {
		this.id = id;
	}

	/**
	 * @return the year
	 */
	public Integer getYear() {
		return year;
	}

	/**
	 * @param year
	 *            the year to set
	 */
	public void setYear(Integer year) {
		this.year = year;
	}

	@Override
	// overriding hashcode method for comparator
	// used sample implementation from
	// http://javarevisited.blogspot.com/2011/10/override-hashcode-in-java-example.html
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((id == null) ? 0 : id.hashCode());
		result = prime * result + ((year == null) ? 0 : year.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}

		if (obj == null) {
			return false;
		}

		if (getClass() != obj.getClass()) {
			return false;
		}

		StationKey other = (StationKey) obj;
		if (id == null) {
			if (other.id != null) {
				return false;
			}
		} else if (!id.equals(other.id))
			return false;
		if (year == null) {
			if (other.year != null){
				return false;
			}
		} else if (!year.equals(other.year)){
			return false;
		}
		return true;

	}

	public int compareTo(StationKey o) {
		return this.id.compareTo(o.id);
	}

	public void readFields(DataInput arg0) throws IOException {
		this.id = arg0.readUTF();
		this.year = arg0.readInt();
	}

	public void write(DataOutput arg0) throws IOException {
		arg0.writeUTF(this.id);
		arg0.writeInt(this.year);
	}

}
