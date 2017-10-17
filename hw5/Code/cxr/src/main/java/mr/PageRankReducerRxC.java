package mr;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Set;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class PageRankReducerRxC 
extends Reducer<IntWritable,DoubleWritable,IntWritable,DoubleWritable> {
	
	public void reduce(IntWritable key, Iterable<DoubleWritable> values,
			Context context) throws IOException, InterruptedException {
		// Add up all the contributions from column id to the reducer and emit as nodeid : pagerank
		Double sum = 0.0;
		for(DoubleWritable val : values) {
			sum += val.get();
		}
	
		context.write(key, new DoubleWritable(sum));
	}
}