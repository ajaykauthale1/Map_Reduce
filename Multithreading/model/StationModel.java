package model;

import util.ProcessUtility;

/** Station Model
 *  records running TMAX count and record count  
 * */

public class StationModel {

	private Double currentTotalTemp;
	private Integer tempCount;

	public StationModel() {
		super();
		currentTotalTemp = 0.0;
		tempCount = 0;
	}


	/**
	 * @return the currentTotalTemp
	 */
	public Double getCurrentTotalTemp() {
		return currentTotalTemp;
	}


	/**
	 * @param currentTotalTemp the currentTotalTemp to set
	 */
	public void setCurrentTotalTemp(Double currentTotalTemp) {
		this.currentTotalTemp = currentTotalTemp;
	}


	/**
	 * @return the tempCount
	 */
	public Integer getTempCount() {
		return tempCount;
	}


	/**
	 * @param tempCount the tempCount to set
	 */
	public void setTempCount(Integer tempCount) {
		this.tempCount = tempCount;
	}


	public void addNextTemperature(Double temp) {
		this.currentTotalTemp += temp;
		this.tempCount++;
	}

	public Double calculateAvgTemp() {
		return (this.currentTotalTemp / this.tempCount);
	}
	
	/**
	 * Method to add the temp in sysnchronized way
	 * 
	 * @param temp temparaturre to be added
	 * @param isFibSelected flag if fobonacci is needed to run in between
	 */
	public synchronized void addTempInSync(Double temp, boolean isFibSelected){
		// if fibonacci is selected for run then execute fibonacci series
		if(isFibSelected) {
			ProcessUtility.fibonacciRun(17);
		}
		
		currentTotalTemp += temp;
		tempCount++;
	}
}
