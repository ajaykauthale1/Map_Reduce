package model;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

/**
 * Station model class used for in map combiner
 * 
 * @author ajay
 *
 */
public class StationModel implements Writable {
	// instance variables for TMAX, TMIN and their respective count
	private Double TMAX;
	private Double TMIN;
	private int TMAXCnt;
	private int TMINCnt;
	
	
	
	/**
	 * 
	 */
	public StationModel() {
		super();
	}

	/**
	 * @param tMAX
	 * @param tMIN
	 * @param tMAXCnt
	 * @param tMINCnt
	 */
	public StationModel(Double tMAX, Double tMIN, int tMAXCnt, int tMINCnt) {
		super();
		TMAX = tMAX;
		TMIN = tMIN;
		TMAXCnt = tMAXCnt;
		TMINCnt = tMINCnt;
	}

	/**
	 * @return the tMAX
	 */
	public Double getTMAX() {
		return TMAX;
	}

	/**
	 * @param tMAX the tMAX to set
	 */
	public void setTMAX(Double tMAX) {
		TMAX = tMAX;
	}

	/**
	 * @return the tMIN
	 */
	public Double getTMIN() {
		return TMIN;
	}

	/**
	 * @param tMIN the tMIN to set
	 */
	public void setTMIN(Double tMIN) {
		TMIN = tMIN;
	}

	/**
	 * @return the tMAXCnt
	 */
	public int getTMAXCnt() {
		return TMAXCnt;
	}

	/**
	 * @param tMAXCnt the tMAXCnt to set
	 */
	public void setTMAXCnt(int tMAXCnt) {
		TMAXCnt = tMAXCnt;
	}

	/**
	 * @return the tMINCnt
	 */
	public int getTMINCnt() {
		return TMINCnt;
	}

	/**
	 * @param tMINCnt the tMINCnt to set
	 */
	public void setTMINCnt(int tMINCnt) {
		TMINCnt = tMINCnt;
	}

	public void readFields(DataInput arg0) throws IOException {
		this.TMAX = arg0.readDouble();
		this.TMIN = arg0.readDouble();
		this.TMAXCnt = arg0.readInt();
		this.TMINCnt = arg0.readInt();
	}

	public void write(DataOutput arg0) throws IOException {
		arg0.writeDouble(this.TMAX);
		arg0.writeDouble(this.TMIN);
		arg0.writeInt(this.TMAXCnt);
		arg0.writeInt(this.TMINCnt);
	}
	
 	@Override
	public String toString() {
	return ", "+TMIN+ ", " + TMAX;
	}
}
