package model;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

/**
 * Station model class used for No_Combiner
 * 
 * @author ajay
 *
 */
public class StationModel implements Writable {
	// Identifier for checking TMIN and TMAX, TMIN - false, TMAX - true
	private boolean isTMAX;
	// Current temperature
	private Double tmp;

	/**
	 * @param isTMAX
	 * @param tmp
	 */
	public StationModel(boolean isTMAX, Double tmp) {
		super();
		this.isTMAX = isTMAX;
		this.tmp = tmp;
	}

	/**
	 * 
	 */
	public StationModel() {
		super();
	}

	/**
	 * @return the isTMAX
	 */
	public boolean isTMAX() {
		return isTMAX;
	}

	/**
	 * @param isTMAX the isTMAX to set
	 */
	public void setTMAX(boolean isTMAX) {
		this.isTMAX = isTMAX;
	}

	/**
	 * @return the tmp
	 */
	public Double getTmp() {
		return tmp;
	}

	/**
	 * @param tmp the tmp to set
	 */
	public void setTmp(Double tmp) {
		this.tmp = tmp;
	}

	@Override
	public void readFields(DataInput arg0) throws IOException {
		this.isTMAX = arg0.readBoolean();
		this.tmp = arg0.readDouble();
	}

	@Override
	public void write(DataOutput arg0) throws IOException {
		arg0.writeBoolean(this.isTMAX);
		arg0.writeDouble(this.tmp);
	}

}
