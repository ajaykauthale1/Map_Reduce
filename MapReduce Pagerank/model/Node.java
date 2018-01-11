/**
 * 
 */
package model;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

/**
 * Model for storing page rank and adjacency list
 * 
 * @author Ajay
 **/
public class Node implements Writable {
	
	private Boolean isPageRank = new Boolean(false);
	private Double pageRankValue = new Double(0.0);
	private Integer outLinkCount = new Integer(0);
	private Text adjacencyList = new Text();
	
	/**
	 * 
	 */
	public Node(){
	}
	
	/**
	 * 
	 * @param isPageRank
	 * @param adjacencyList
	 */
	public Node(Boolean isPageRank, String adjacencyList) {
		super();
		this.isPageRank = isPageRank;
		this.adjacencyList.set(adjacencyList);
		this.pageRankValue = 0.0;
		this.outLinkCount = 0;
	}
	
	/**
	 * 
	 * @param isPageRank
	 * @param pageRankValue
	 * @param outLinkCount
	 */
	public Node(Boolean isPageRank, Double pageRankValue,
			Integer outLinkCount) {
		super();
		this.isPageRank = isPageRank;
		this.pageRankValue = pageRankValue;
		this.outLinkCount = outLinkCount;
		this.adjacencyList = new Text();
	}

	

	/**
	 * @return the isPageRank
	 */
	public Boolean getIsPageRank() {
		return isPageRank;
	}

	/**
	 * @param isPageRank the isPageRank to set
	 */
	public void setIsPageRank(Boolean isPageRank) {
		this.isPageRank = isPageRank;
	}

	/**
	 * @return the pageRankValue
	 */
	public Double getPageRankValue() {
		return pageRankValue;
	}

	/**
	 * @param pageRankValue the pageRankValue to set
	 */
	public void setPageRankValue(Double pageRankValue) {
		this.pageRankValue = pageRankValue;
	}

	/**
	 * @return the outLinkCount
	 */
	public Integer getOutLinkCount() {
		return outLinkCount;
	}

	/**
	 * @param outLinkCount the outLinkCount to set
	 */
	public void setOutLinkCount(Integer outLinkCount) {
		this.outLinkCount = outLinkCount;
	}

	/**
	 * @return the adjacencyList
	 */
	public Text getAdjacencyList() {
		return adjacencyList;
	}

	/**
	 * @param adjacencyList the adjacencyList to set
	 */
	public void setAdjacencyList(Text adjacencyList) {
		this.adjacencyList = adjacencyList;
	}
	
	public void write(DataOutput obj) throws IOException {
		obj.writeBoolean(this.isPageRank);
		obj.writeDouble(this.pageRankValue);
		obj.writeInt(this.outLinkCount);
		this.adjacencyList.write(obj);
	}


	public void readFields(DataInput obj) throws IOException {
		
		this.isPageRank = obj.readBoolean();
		this.pageRankValue = obj.readDouble();
		this.outLinkCount = obj.readInt();
		this.adjacencyList.readFields(obj);
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result
				+ ((adjacencyList == null) ? 0 : adjacencyList.hashCode());
		result = prime * result
				+ ((outLinkCount == null) ? 0 : outLinkCount.hashCode());
		result = prime
				* result
				+ ((isPageRank == null) ? 0
						: isPageRank.hashCode());
		result = prime * result
				+ ((pageRankValue == null) ? 0 : pageRankValue.hashCode());
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
		Node other = (Node) obj;
		if (adjacencyList == null) {
			if (other.adjacencyList != null)
				return false;
		} else if (!adjacencyList.toString().equals(other.adjacencyList.toString()))
			return false;
		if (outLinkCount == null) {
			if (other.outLinkCount != null)
				return false;
		} else if (!outLinkCount.equals(other.outLinkCount))
			return false;
		if (isPageRank == null) {
			if (other.isPageRank != null)
				return false;
		} else if (!isPageRank
				.equals(other.isPageRank))
			return false;
		if (pageRankValue == null) {
			if (other.pageRankValue != null)
				return false;
		} else if (!pageRankValue.equals(other.pageRankValue))
			return false;
		return true;
	}

	
}
