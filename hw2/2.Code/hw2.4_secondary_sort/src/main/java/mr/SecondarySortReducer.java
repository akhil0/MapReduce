package mr;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SecondarySortReducer
extends Reducer<CompositeKeyWritable,CompositeValueWritable,Text,Text> {

	public void reduce(CompositeKeyWritable key, Iterable<CompositeValueWritable> values,
			Context context) throws IOException, InterruptedException {

		// List stores the tmax avg, tmin avg for each year as String
		List<String> valuelist = new ArrayList<String>();

		// year, which can be used for checking if next year is same or not.
		String year = new String();

		CompositeValueWritable tempobj = new CompositeValueWritable();

		// Iterate over each iterable to calculate accordingly
		for(CompositeValueWritable valobj : values) {
			
			// first time in iterable, will go through this condition,
			// sets the temporary object to current object and updates null
			// year to current year.
			if(year.isEmpty()) {
				tempobj.setTmaxCount(valobj.getTmaxCount());
				tempobj.setTmaxTemp(valobj.getTmaxTemp());
				tempobj.setYear(valobj.getYear());
				tempobj.setTminTemp(valobj.getTminTemp());
				tempobj.setTminCount(valobj.getTminCount());
				year = valobj.getYear();
			}
			// if the year of the current object is same as the year, meaning
			// the objects belong to same year. In this case, just update
			// the value of temporary object by adding all fields to
			// the current object
			else if(year.equals(valobj.getYear())) {
				tempobj.setTmaxCount(valobj.getTmaxCount() + tempobj.getTmaxCount());
				tempobj.setTmaxTemp(valobj.getTmaxTemp() + tempobj.getTmaxTemp());
				tempobj.setTminTemp(valobj.getTminTemp() + tempobj.getTminTemp());
				tempobj.setTminCount(valobj.getTminCount() + tempobj.getTminCount());
			}
			// if the year of the current object is different as year, meaning
			// the object belongs to different year. In this case, find the avg
			// String and add it to arraylist. And we need to initialize the temp
			// again and set it to the values to current object.
			else {
				valuelist.add(calculateAverage(tempobj));
				tempobj = new CompositeValueWritable(valobj.getYear(),
						valobj.getTmaxTemp(),valobj.getTmaxCount(),
						valobj.getTminTemp(),valobj.getTminCount());
				year = valobj.getYear();
			}
		}
		
		//This computation step is for the last set of same year objects,
		//since year wont change after that, so we need to add it to the list
		// after the loop.
		valuelist.add(calculateAverage(tempobj));
		
		// Converting the whole Arraylist to Arrays and then to a string
		String result = Arrays.toString(valuelist.toArray());
		
		// emit the key.natural key and result
		context.write(new Text(key.getStationId()), new Text(result));
	}

	
	// takes the composite value writable, and returns a string which
	// concatenates the tmaxavg, tminavg and the year.
	public String calculateAverage(CompositeValueWritable val){
		String result = "";
		Double tmaxavg = null;
		Double tminavg = null;
		// if tmax count is 0, it means the stations counting are 0, return null
		if(val.getTmaxCount() == 0){}
		else
			tmaxavg = val.getTmaxTemp()/(double)val.getTmaxCount();
		// if tmin count is 0, it means the stations counting are 0, return null
		if(val.getTminCount() == 0){}
		else
			tminavg = val.getTminTemp()/(double)val.getTminCount();
		result += "(" + val.getYear() + ", " + tminavg + ", " + tmaxavg + ")";
		return result;
	}
}
