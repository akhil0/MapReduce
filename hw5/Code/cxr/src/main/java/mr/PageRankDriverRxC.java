package mr;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;





public class PageRankDriverRxC {

	public enum PageRankCounters {
		TOTAL_NODES,
		DANGLING_VALUE
	}

	public static void main(String[] args) throws IOException, Exception {

		// Build Index Job
		Configuration conf = new Configuration();
		conf.set("mapred.textoutputformat.separator", "~~");
		System.setProperty("hadoop.home.dir", "/");
		Job job = Job.getInstance(conf, "build-index");
		job.setNumReduceTasks(1);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		Path out = new Path(args[1] + "/index");
		FileOutputFormat.setOutputPath(job, out);
		out.getFileSystem(conf).delete(out, true);
		job.setJarByClass(PageRankDriverRxC.class);
		job.setMapperClass(IndexMapper.class);
		job.setReducerClass(IndexReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.waitForCompletion(true);
		long totalnodes = job.getCounters().findCounter(PageRankCounters.TOTAL_NODES).getValue();
		conf.setLong("matrix.pagerank.totalnodes", totalnodes);

		// Build M matrix
		Job job2 = Job.getInstance(conf, "build-matrix");
		FileInputFormat.addInputPath(job2, new Path(args[0]));
		Path outPath = new Path(args[1] + "/buildMatrix");
		FileOutputFormat.setOutputPath(job2, outPath);
		outPath.getFileSystem(conf).delete(outPath, true);
		Path directoryPath = new Path(new URI(args[1] + "/index"));
        FileSystem fs = directoryPath.getFileSystem(conf);
        FileStatus[] fileStatus = fs.listStatus(directoryPath);
        for (FileStatus status : fileStatus) {
            job2.addCacheFile(status.getPath().toUri());
        }
		//job2.addCacheFile(new URI(args[1] + "/index/part-r-00000"));
		job2.setJarByClass(PageRankDriverRxC.class);
		job2.setMapperClass(BuildMatrixMapper.class);
		job2.setMapOutputKeyClass(Text.class);
		job2.setMapOutputValueClass(Text.class);
		job2.setReducerClass(BuildMatrixReducer.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);
		job2.setNumReduceTasks(1);
		job2.waitForCompletion(true);

		// Build Rank Matrix
		Job job3 = Job.getInstance(conf, "build-rank-matrix");
		Path outPath23 = new Path(args[1] + "/buildMatrix");
		FileInputFormat.addInputPath(job3, outPath23);
		Path outPath1 = new Path(args[1] + "/finalresult0");
		FileOutputFormat.setOutputPath(job3, outPath1);
		outPath1.getFileSystem(conf).delete(outPath1, true);
		job3.setJarByClass(PageRankDriverRxC.class);
		job3.setMapperClass(RankMatrixMapper.class);
		job3.setNumReduceTasks(0);
		job3.setMapOutputKeyClass(Text.class);
		job3.setMapOutputValueClass(Text.class);
		job3.waitForCompletion(true);
		
		//RowxColumn Page Iteration
		for(int iteration = 1; iteration <= 10 ; iteration++) {
			Job job4 = Job.getInstance(conf, "rowxcolumn");
			Path outPath33 = new Path(args[1] + "/buildMatrix");
			FileInputFormat.addInputPath(job4, outPath33);
			Path outPath2 = new Path(args[1] + "/tempresult" + iteration);
			FileOutputFormat.setOutputPath(job4, outPath2);
			outPath2.getFileSystem(conf).delete(outPath2, true);
			Path directoryPathnew = new Path(new URI(args[1] + "/finalresult" + (iteration-1)));
            FileSystem fsnew = directoryPathnew.getFileSystem(conf);
            FileStatus[] fileStatusnew = fsnew.listStatus(directoryPathnew);
            for (FileStatus status : fileStatusnew) {
                job4.addCacheFile(status.getPath().toUri());
            }
			//job4.addCacheFile(new URI(args[1] + "/finalresult" + (iteration-1) + "/part-r-00000"));
			job4.setJarByClass(PageRankDriverRxC.class);
			job4.setMapperClass(PageRankMapperRxC.class);
			job4.setMapOutputKeyClass(IntWritable.class);
			job4.setMapOutputValueClass(DoubleWritable.class);
			job4.setReducerClass(PageRankReducerRxC.class);
			job4.setOutputKeyClass(IntWritable.class);
			job4.setOutputValueClass(DoubleWritable.class);
			job4.waitForCompletion(true);
			conf.setLong("pagerank.delta", job4.getCounters().findCounter(PageRankCounters.DANGLING_VALUE).getValue());
			job4.getCounters().findCounter(PageRankCounters.DANGLING_VALUE).setValue(0);
			
			// Dangling Node Handling Job
			Job job5 = Job.getInstance(conf, "final iteration");
			Path newinputpath = new Path(args[1] + "/finalresult" + (iteration-1));
			FileInputFormat.addInputPath(job5, newinputpath);
			Path outPath3 = new Path(args[1] + "/finalresult" + iteration);
			FileOutputFormat.setOutputPath(job5, outPath3);
			outPath3.getFileSystem(conf).delete(outPath3, true);
			Path directoryPath1 = new Path(new URI(args[1] + "/tempresult" + iteration));
            FileSystem fs1 = directoryPath1.getFileSystem(conf);
            FileStatus[] fileStatus1 = fs1.listStatus(directoryPath1);
            for (FileStatus status : fileStatus1) {
                job5.addCacheFile(status.getPath().toUri());
            }
			//job5.addCacheFile(new URI(args[1] + "/tempresult" + iteration + "/part-r-00000"));
			job5.setJarByClass(PageRankDriverRxC.class);
			job5.setMapperClass(DanglingMapper.class);
			job5.setMapOutputKeyClass(IntWritable.class);
			job5.setMapOutputValueClass(DoubleWritable.class);
			job5.waitForCompletion(true);
		}

		// Top-100 Job
		Job job6 = Job.getInstance(conf, "top-100 records");
		Path in = new Path(args[1] + "/finalresult10");
		FileInputFormat.addInputPath(job6, in);
		Path outPath11 = new Path(args[1] + "/top100pagerank");
		FileOutputFormat.setOutputPath(job6, outPath11);
		outPath11.getFileSystem(conf).delete(outPath11, true);
		Path directoryPath2 = new Path(new URI(args[1] + "/index"));
        FileSystem fs2 = directoryPath2.getFileSystem(conf);
        FileStatus[] fileStatus2 = fs2.listStatus(directoryPath2);
        for (FileStatus status : fileStatus2) {
            job6.addCacheFile(status.getPath().toUri());
        }
		//job6.addCacheFile(new URI(args[1] + "/index/part-r-00000"));
		job6.setJarByClass(PageRankDriverRxC.class);
		job6.setMapperClass(Top100Mapper.class);
		job6.setMapOutputKeyClass(RankNode.class);
		job6.setMapOutputValueClass(DoubleWritable.class);
		job6.setReducerClass(Top100Reducer.class);
		job6.setOutputKeyClass(NullWritable.class);
		job6.setOutputValueClass(Text.class);
		job6.setNumReduceTasks(1);
		System.exit(job6.waitForCompletion(true) ? 0 : 1);
	}

}
