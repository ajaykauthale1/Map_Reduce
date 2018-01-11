/**
 * 
 */
package model;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

/**
 * Model for reducer of to emit TOP K records in descending order of page rank value
 * 
 * @author Ajay
 **/
public class Record implements Writable {
	// To store node
	private String node;
	// To store page rank value
	private Double pageRank;
	
	/**
	 * 
	 */
	public Record() {
		super();
	}

	/**
	 * 
	 * @param node
	 * @param pageRank
	 */
	public Record(String node, Double pageRank) {
		super();
		this.node = node;
		this.pageRank = pageRank;
	}

	
	/**
	 * @return the node
	 */
	public String getNode() {
		return node;
	}

	/**
	 * @param node the node to set
	 */
	public void setNode(String node) {
		this.node = node;
	}

	/**
	 * @return the pageRank
	 */
	public Double getPageRank() {
		return pageRank;
	}

	/**
	 * @param pageRank the pageRank to set
	 */
	public void setPageRank(Double pageRank) {
		this.pageRank = pageRank;
	}

	public void write(DataOutput obj01) throws IOException {
		obj01.writeUTF(this.node);
		obj01.writeDouble(this.pageRank);
	}

	public void readFields(DataInput obj01) throws IOException {
		this.node = obj01.readUTF();
		this.pageRank = obj01.readDouble();
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((node == null) ? 0 : node.hashCode());
		result = prime * result
				+ ((pageRank == null) ? 0 : pageRank.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Record other = (Record) obj;
		if (node == null) {
			if (other.node != null)
				return false;
		} else if (!node.equals(other.node))
			return false;
		if (pageRank == null) {
			if (other.pageRank != null)
				return false;
		} else if (!pageRank.equals(other.pageRank))
			return false;
		return true;
	}

}