package mr;


import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;


public class CompositeKeyWritable implements Writable,
WritableComparable<CompositeKeyWritable> {

	
	// Composite Key Object has natural key as stationid, natural value as year
	private String stationid;
	private String year;

	public CompositeKeyWritable() {
	}

	public CompositeKeyWritable(String stationid, String year) {
		this.stationid = stationid;
		this.year = year;

	}

	// sets stationid for the object
	public void setStationId(String stationid) {
		this.stationid = stationid;
	}

	//gets stationid from the object
	public String getStationId(){
		return stationid;
	}

	// sets the year for the object
	public void setYear(String year){
		this.year = year;
	}

	// gets the year from the object
	public String getYear(){
		return year;
	}

	// reads the dataInput in the order
	public void readFields(DataInput dataInput) throws IOException {
		stationid = WritableUtils.readString(dataInput);
		year = WritableUtils.readString(dataInput);
	}

	// writes to data Output in the order
	public void write(DataOutput dataOutput) throws IOException {
		WritableUtils.writeString(dataOutput, stationid);
		WritableUtils.writeString(dataOutput, year);
	}


	public int compareTo(CompositeKeyWritable o) {
		// TODO Auto-generated method stub
		int result = stationid.compareTo(o.getStationId());
		if (0 == result) {
			result = year.compareTo(o.getYear());
		}
		return result;
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((stationid == null) ? 0 : stationid.hashCode());
		result = prime * result + ((year == null) ? 0 : year.hashCode());
		return result;
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		CompositeKeyWritable other = (CompositeKeyWritable) obj;
		if (stationid == null) {
			if (other.stationid != null)
				return false;
		} else if (!stationid.equals(other.stationid))
			return false;
		if (year == null) {
			if (other.year != null)
				return false;
		} else if (!year.equals(other.year))
			return false;
		return true;
	}


}
