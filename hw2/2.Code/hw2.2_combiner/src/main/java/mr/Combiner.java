package mr;


import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;



public class Combiner {

	public static class CustomMapper
	extends Mapper<Object, Text, Text, CompositeValueWritable>{

		public void map(Object key, Text value, Context context
				) throws IOException, InterruptedException {
			Text mkey = new Text();
			String parts[] = value.toString().split(",");
			// initalize new object with tmax fields accordingly
			if(parts[2].equals("TMAX")){
				CompositeValueWritable newobj = new 
						CompositeValueWritable(Integer.parseInt(parts[3]),1,0,0);
				mkey.set(parts[0]);
				context.write(mkey,newobj);
			}
			// initialize new object with tmin fields accordingly
			else if (parts[2].equals("TMIN")){
				CompositeValueWritable newobj = new 
						CompositeValueWritable(0,0,Integer.parseInt(parts[3]),1);
				mkey.set(parts[0]);
				context.write(mkey,newobj);
			}
		}
	}

	public static class CustomCombiner
	extends Reducer<Text,CompositeValueWritable,Text,CompositeValueWritable> {

		public void reduce(Text key, Iterable<CompositeValueWritable> values,
				Context context) throws IOException, InterruptedException {
			int tmaxsum = 0;
			int tminsum = 0;
			int tmaxcount = 0;
			int tmincount = 0;

			// Go through iterable, and aggregate all objects on same fields,
			// the final value will be total tmax sum, total tmax count, total tmin
			// sum, total tmin count which have same stationid (grouped)
			for (CompositeValueWritable val : values) {
				tmaxsum += val.getTmaxTemp();
				tminsum += val.getTminTemp();
				tmaxcount += val.getTmaxCount();
				tmincount += val.getTminCount();
			}
			
			// returns map output format value object with updated fields
			CompositeValueWritable valobj = new 
					CompositeValueWritable(tmaxsum,tmaxcount,tminsum,tmincount);
			context.write(key, valobj);
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
		Job job = Job.getInstance(conf, "no combiner");

		FileInputFormat.setInputPaths(job, new Path(args[0]));

		Path outPath = new Path(args[1]);
		FileOutputFormat.setOutputPath(job, outPath);
		outPath.getFileSystem(conf).delete(outPath, true);

		job.setJarByClass(Combiner.class);

		job.setMapperClass(CustomMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(CompositeValueWritable.class);

		job.setCombinerClass(CustomCombiner.class);

		job.setReducerClass(CustomReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);


		System.exit(job.waitForCompletion(true) ? 0 : 1);


	}
}