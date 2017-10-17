package mr;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;



public class SecondarySortDriver {

	public static void main(String[] args) throws Exception {
		
		Configuration conf = new Configuration();
		conf.set("mapred.textoutputformat.separator", ",");
		System.setProperty("hadoop.home.dir", "/");
		Job job = Job.getInstance(conf, "secondary sort");

		FileInputFormat.addInputPath(job, new Path(args[0]));

		Path outPath = new Path(args[1]);
		FileOutputFormat.setOutputPath(job, outPath);
		outPath.getFileSystem(conf).delete(outPath, true);

		job.setJarByClass(SecondarySortDriver.class);

		job.setMapperClass(SecondarySortMapper.class);
		job.setMapOutputKeyClass(CompositeKeyWritable.class);
		job.setMapOutputValueClass(CompositeValueWritable.class);
		job.setPartitionerClass(SecondarySortPartitioner.class);
		job.setGroupingComparatorClass(SecondarySortGroupingComparator.class);
		job.setSortComparatorClass(SecondarySortKeyComparator.class);
		job.setReducerClass(SecondarySortReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		//conf.set("mapreduce.output.key.field.separator", ",");
		
		job.setNumReduceTasks(10);
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);


	}
}