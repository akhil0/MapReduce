package mr;

import java.io.IOException;
import java.util.PriorityQueue;
import java.util.Queue;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;



public class Top100Mapper 
extends Mapper<LongWritable, Text, RankNode, DoubleWritable>{

	// Priority Queue which stores in order of node.pagerank 
	Queue<RankNode> rankqueue = new PriorityQueue<>();

	public void map(LongWritable key, Text value,
			Context context)
					throws IOException, InterruptedException {
		String[] parts = value.toString().split("~~");
		Double pagerank = Double.parseDouble(parts[1]);

		rankqueue.offer(new RankNode(parts[0], pagerank));
	}

	protected void cleanup(Context context) throws IOException, InterruptedException {	

		// emit out 100 first items from queue
		int count = 99;
		while(count>=0 && !rankqueue.isEmpty()){
			RankNode node = rankqueue.poll();
			context.write(node, new DoubleWritable(node.pageRank));
			count--;
		}
	}
}
