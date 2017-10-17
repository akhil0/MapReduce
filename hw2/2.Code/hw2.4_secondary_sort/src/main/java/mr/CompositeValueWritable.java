package mr;


import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;



public class CompositeValueWritable implements Writable {

	// year : acts as key for temp calculations in reduce part as identifier 
	// composite key
	// tmaxval : stores the sum of tmax values so far
	// tminval : stores the sum of tmin values so far
	// tmaxcount : stores the count of stations which have tmax
	// tmincount : stores the count of stations which have tmin
	private String year;
	private int tmaxval;
	private int tmaxcount;
	private int tminval;
	private int tmincount;

	public CompositeValueWritable() {
	}

	public CompositeValueWritable(String year, int tmaxval, 
			int tmaxcount, int tminval, int tmincount) {
		this.year = year;
		this.tmaxval = tmaxval;
		this.tmaxcount = tmaxcount;
		this.tminval = tminval;
		this.tmincount = tmincount;
	}

	// gets Tmax Temp sum so far (aggregate)
	public int getTmaxTemp() {
		return tmaxval;
	}

	// sets Tmax Temp sum
	public void setTmaxTemp(int tmaxval) {
		this.tmaxval = tmaxval;
	}

	// sets the count of stations which have Tmax
	public void setTmaxCount(int tmaxcount) {
		this.tmaxcount = tmaxcount;
	}

	// gets the count of stations which have Tmax
	public int getTmaxCount(){
		return tmaxcount;
	}

	// gets Tmin Temp sum so far (aggregate)
	public int getTminTemp() {
		return tminval;
	}

	// sets Tmin Temp sum so far (aggregate)
	public void setTminTemp(int tminval) {
		this.tminval = tminval;
	}

	// sets the count of stations which have Tmin
	public void setTminCount(int tmincount) {
		this.tmincount = tmincount;
	}

	// gets the count of stations which have Tmin
	public int getTminCount(){
		return tmincount;
	}

	public void setYear(String year) {
		this.year = year;
	}

	public String getYear(){
		return year;
	}

	@Override
	public String toString() {
		return (new StringBuilder().append(year).append("\t")
				.append(tmaxval).append("\t").append(tmaxcount).append("\t")
				.append(tminval).append("\t").append(tmincount)).toString();
	}


	// Reads the data input into object in the order
	public void readFields(DataInput dataInput) throws IOException {
		year = WritableUtils.readString(dataInput);
		tmaxval = WritableUtils.readVInt(dataInput);
		tmaxcount = WritableUtils.readVInt(dataInput);
		tminval = WritableUtils.readVInt(dataInput);
		tmincount = WritableUtils.readVInt(dataInput);
	}

	// Writes the data output into object in order
	public void write(DataOutput dataOutput) throws IOException {
		WritableUtils.writeString(dataOutput, year);
		WritableUtils.writeVInt(dataOutput, tmaxval);
		WritableUtils.writeVInt(dataOutput, tmaxcount);
		WritableUtils.writeVInt(dataOutput, tminval);
		WritableUtils.writeVInt(dataOutput, tmincount);
	}
}
