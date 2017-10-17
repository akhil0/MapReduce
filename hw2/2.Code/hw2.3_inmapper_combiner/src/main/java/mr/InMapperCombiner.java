package mr;


import java.io.IOException;
import java.util.HashMap;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class InMapperCombiner {

	public static class CustomMapper
	extends Mapper<Object, Text, Text, CompositeValueWritable>{

		CompositeValueWritable newobj = new CompositeValueWritable();
		Text mkey = new Text();

		// in mapper data structure which allows us to reduce same stationid
		//objects on map task itself, there by reducing the no. of reduce input
		// records.
		HashMap<String, CompositeValueWritable> valuemap = new 
				HashMap<String, CompositeValueWritable>();

		public void map(Object key, Text value, Context context
				) throws IOException, InterruptedException {

			String parts[] = value.toString().split(",");
			
			// update tmax fields in value object based on the type.
			if(parts[2].equals("TMAX")) {
				int val = Integer.parseInt(parts[3]);
				// if the shared data structure has key, update the existing object
				if(valuemap.containsKey(parts[0])) {
					CompositeValueWritable tempvalobj = valuemap.get(parts[0]);
					tempvalobj.setTmaxTemp(tempvalobj.getTmaxTemp() + val);
					tempvalobj.setTmaxCount(tempvalobj.getTmaxCount() + 1);
				}
				// create a new object and put it in hashmap
				else {
					CompositeValueWritable tempvalobj = new 
							CompositeValueWritable(val,1,0,0);
					valuemap.put(parts[0], tempvalobj);
				}
			}
			// update tmin fields in value object based on the type.
			else if (parts[2].equals("TMIN")) {
				int val = Integer.parseInt(parts[3]);
				// if the shared data structure has key, update the existing object
				if(valuemap.containsKey(parts[0])) {
					CompositeValueWritable tempvalobj = valuemap.get(parts[0]);
					tempvalobj.setTminCount(tempvalobj.getTminCount() + 1);
					tempvalobj.setTminTemp(tempvalobj.getTminTemp() + val);
				}
				// create a new object and put it in hashmap
				else {
					CompositeValueWritable tempvalobj = new 
							CompositeValueWritable(0,0,val,1);
					valuemap.put(parts[0], tempvalobj);
				}
			}
		}

		// emits each (k,v) of map task data structure
		public void cleanup(Context context)
				throws IOException, InterruptedException {
			Set<String> valuekeyset = valuemap.keySet();
			for(String s : valuekeyset) {
				context.write(new Text(s), valuemap.get(s));
			}
		}
	}

	public static class CustomReducer
	extends Reducer<Text,CompositeValueWritable,Text,Text> {

		public void reduce(Text key, Iterable<CompositeValueWritable> values,
				Context context) throws IOException, InterruptedException {

			int tmaxsum = 0;
			int tminsum = 0;
			int tmaxcount = 0;
			int tmincount = 0;

			// Go through iterable, and aggregate all objects on same fields,
			// the final value will be total tmax sum, total tmax count, total tmin
			// sum, total tmin count.
			for (CompositeValueWritable val : values) {
				tmaxsum += val.getTmaxTemp();
				tminsum += val.getTminTemp();
				tmaxcount += val.getTmaxCount();
				tmincount += val.getTminCount();
			}
			
			// find tmax avg and tmin avg
			double tmaxavg = tmaxsum/(double)tmaxcount;
			double tminavg = tminsum/(double)tmincount;
			String avg = String.valueOf(tminavg) + ", " + String.valueOf(tmaxavg);
			context.write(new Text(key), new Text(avg));

		}
	}


	public static void main(String[] args) throws Exception {


		Configuration conf = new Configuration();
		conf.set("mapred.textoutputformat.separator", ",");
		System.setProperty("hadoop.home.dir", "/");
		Job job = Job.getInstance(conf, "in mapper combiner");

		FileInputFormat.setInputPaths(job, new Path(args[0]));

		Path outPath = new Path(args[1]);
		FileOutputFormat.setOutputPath(job, outPath);
		outPath.getFileSystem(conf).delete(outPath, true);

		job.setJarByClass(InMapperCombiner.class);

		job.setMapperClass(CustomMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(CompositeValueWritable.class);

		job.setReducerClass(CustomReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);


		System.exit(job.waitForCompletion(true) ? 0 : 1);


	}
}