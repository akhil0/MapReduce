package mr;

import java.io.IOException;
import java.util.HashMap;
import java.util.Set;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SecondarySortMapper extends 
	Mapper<Object, Text, CompositeKeyWritable, CompositeValueWritable>{
	
	HashMap<CompositeKeyWritable, CompositeValueWritable> valuemap = new 
			HashMap<CompositeKeyWritable, CompositeValueWritable>();
	
	public void map(Object key, Text value, Context context
			) throws IOException, InterruptedException {

		String parts[] = value.toString().split(",");
		String year = parts[1].substring(0, 4);
		String stationid = parts[0];
		
		// update tmax fields accordingly
		if(parts[2].equals("TMAX")) {
			int val = Integer.parseInt(parts[3]);
			CompositeKeyWritable checkkeyobj = new CompositeKeyWritable(stationid, year);
			// If object already exists, update tmax fields accordingly
			if(valuemap.containsKey(checkkeyobj)) {
				CompositeValueWritable tempvalobj = valuemap.get(checkkeyobj);
				tempvalobj.setTmaxTemp(tempvalobj.getTmaxTemp() + val);
				tempvalobj.setTmaxCount(tempvalobj.getTmaxCount() + 1);
			}
			// Create new Object with tmax value set and count 1.
			else {
				CompositeValueWritable tempvalobj = new 
						CompositeValueWritable(year,val,1,0,0);
				CompositeKeyWritable tempkeyobj = new 
						CompositeKeyWritable(stationid,year);
				valuemap.put(tempkeyobj, tempvalobj);
			}
		}
		// update tmin fields accordingly
		else if (parts[2].equals("TMIN")) {
			int val = Integer.parseInt(parts[3]);
			CompositeKeyWritable checkkeyobj = new CompositeKeyWritable(stationid,year);
			// If object already exists, update tmax fields accordingly
			if(valuemap.containsKey(checkkeyobj)) {
				CompositeValueWritable tempvalobj = valuemap.get(checkkeyobj);
				tempvalobj.setTminCount(tempvalobj.getTminCount() + 1);
				tempvalobj.setTminTemp(tempvalobj.getTminTemp() + val);
			}
			// Create new Object with tmin value set and count 1.
			else {
				CompositeValueWritable tempvalobj = new 
						CompositeValueWritable(year,0,0,val,1);
				CompositeKeyWritable tempkeyobj = new 
						CompositeKeyWritable(stationid,year);
				valuemap.put(tempkeyobj, tempvalobj);
			}
		}
	}
	
	
	public void cleanup(Context context)
			  throws IOException, InterruptedException {
		Set<CompositeKeyWritable> valuekeyset = valuemap.keySet();
		// emits each key and corresponding map value
		for(CompositeKeyWritable s : valuekeyset) {
			context.write(s, valuemap.get(s));
		}
	}
}
