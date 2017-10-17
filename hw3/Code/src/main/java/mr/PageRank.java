package mr;

import java.io.IOException;
import java.util.Arrays;
import java.util.PriorityQueue;
import java.util.Queue;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;



public class PageRank {

	// Counters enumeration for holding total no of nodes and dangling loss after
	// each iteration
	public enum PageRankCounters {
		TOTAL_NODES,
		DANGLING_VALUE
	}

	public static class PreProcessingMapper
	extends Mapper<Object, Text, Text, Text>{
		
		public void map(Object key, Text value, Context context
				) throws IOException, InterruptedException {
			String line = value.toString();
			
			Bz2WikiParser wikiparser = new Bz2WikiParser();
			
			// Returns adjacency nodes list
			String adjlist = wikiparser.getAdjList(line);			
			if(adjlist == null) {
			}
			else{
				// update counter for total no of nodes.
				// only consider nodes that are traversed
				context.getCounter(PageRankCounters.TOTAL_NODES).increment(1);
				String parts[] = adjlist.split("``");
				context.write(new Text(parts[0]), new Text(parts[1]));
			}
		}
	}


	public static class PageRankMapper
	extends Mapper<LongWritable, Text, Text, Node>{

		public void map(LongWritable key, Text value, Context context
				) throws IOException, InterruptedException {

			// 
			String line = value.toString();
			String[] parts = line.split("``");
			
			String nodeid = parts[0];
			
			// Strips off "[,]" from ends of string
			String adjnodestring = parts[1].substring(1,parts[1].length()-1);
			Double pagerank = null;

			// Check if first iteration, i.e only 2 parts after split
			if(parts.length == 2){
				// First iterations page rank is unknown, so set to 1/V
				pagerank = (double)1/context.getConfiguration().getLong("pagerank.totalnodes",0);
			}
			else {
				// Remaining iterations, pagerank can be read from file from parts[2]
				pagerank = Double.parseDouble(parts[2]);
				
				// Updadte the dangling loss happened in previous iterations
				// and add it as normalized value to each node.
				double pagerankloss = (context.getConfiguration().getLong("pagerank.delta",0))/(double)(Math.pow(10,8));

				Double counter = pagerankloss
						/context.getConfiguration().getLong("pagerank.totalnodes",0);
				pagerank += counter;
			}
			
			// No Adjacency List, is primary dangling node. For this case,
			// Node will be set with no adjacency list and emitted.
			// So that we can construct graph at reducer and identify that 
			// its primary dangling node
			if(adjnodestring.length() == 0) {
				context.write(new Text(nodeid), new Node(pagerank, new String[]{}, true));
			}
			// Primary Nodes, containing adjacency list. 
			else{
				String[] adjnodes = adjnodestring.split(", ");
				
				// Emit the node as it is for constrcuting the graph again.
				context.write(new Text(nodeid), new Node(pagerank, adjnodes, true));
				
				//For each node in adjacency list, emit the contribution from
				// this node. 
				for(String adjnodeid : adjnodes) {
					context.write(new Text(adjnodeid), new Node(pagerank/adjnodes.length, new String[]{}, false));
				}
			}

		}
	}

	public static class PageRankReducer
	extends Reducer<Text,Node,Text,Text> {

		public void reduce(Text key, Iterable<Node> values, Context context)
				throws IOException, InterruptedException {
			Double summedPageRank = 0.0;
			Node newnode = null;
			
			double dampening_factor = 0.15;
			long totalnodes = context.getConfiguration().getLong("pagerank.totalnodes",0);
			
			// Iterate through all values
			for(Node node : values) {
				//	System.out.println(node.getPageRank() + Arrays.toString(node.));
				//If node is primary node, update the node to current node
				if(node.getPrimaryNode() && newnode == null){
					newnode = new Node(node.getPageRank(), node.getAdjNodeList(), 
							node.getPrimaryNode());
				} 
				// Keep updating total pagerank if otherwise
				else {
					summedPageRank += node.getPageRank();
				}
			}

			// update the summed page rank with dampening factor 
			summedPageRank = (dampening_factor/totalnodes) + (1 - dampening_factor)*summedPageRank;
			StringBuilder sb = new StringBuilder();
			
			// Node is primary node or primary dangling node.
			if(newnode != null) {
				// If adjacency list is 0 size, then update the dangling counter
				// with node's previous pagerank.
				if(newnode.getAdjNodeList().length == 0) {
					//update counter for primary dangling
					long counter = (long) (Math.pow(10, 8) * newnode.getPageRank());
					context.getCounter(PageRankCounters.DANGLING_VALUE).increment(counter);
				}
				// write the node id, adjacency list and pagerank to disk
				// to use for next iteration
				sb.append(Arrays.toString(newnode.getAdjNodeList()));
				sb.append("``").append(String.valueOf(summedPageRank));
				context.write(key, new Text(sb.toString()));
			} 
			// Node will be null considering this node is not primary node
			// or primary dangling node
			else {
				//update counter for secondary dangling
				long counter = (long) (Math.pow(10, 8) * summedPageRank);
				context.getCounter(PageRankCounters.DANGLING_VALUE).increment(counter);
			}
		}
	}


	// Custom class for priortity queue
	public static class RankNode implements Comparable<RankNode>{
		public RankNode(String nodeid, double pagerank){
			this.nodeid = nodeid;
			this.pageRank = pagerank;
		}
		public String nodeid;
		public double pageRank;

		// Compares pageranks
		@Override
		public int compareTo(RankNode o) {
			return Double.compare(o.pageRank, this.pageRank);
		}
	}


	public static class Top100Mapper
	extends Mapper<LongWritable, Text, NullWritable, Text>{

		// Priority Queue which stores in order of node.pagerank 
		Queue<RankNode> rankqueue = new PriorityQueue<>();

		public void map(LongWritable key, Text value,
				Context context)
						throws IOException, InterruptedException {
			String[] parts = value.toString().split("``");
			Double pagerank = Double.parseDouble(parts[2]);
			
			// update the dangling loss from 10th iteration
			double pagerankloss = Math.pow(10, -7) * context.getConfiguration().getLong("pagerank.delta",0);
			Double counter = pagerankloss
					/context.getConfiguration().getLong("pagerank.totalnodes",0);
			pagerank += counter;
			
			rankqueue.offer(new RankNode(parts[0], pagerank));
		}

		protected void cleanup(Context context) throws IOException, InterruptedException {	
			
			// emit out 100 first items from queue
			int count = 99;
			while(count>=0 && !rankqueue.isEmpty()){
				RankNode node = rankqueue.poll();
				context.write(NullWritable.get(), new Text(node.nodeid + "``" + node.pageRank));
				count--;
			}
		}
	}


	public static class Top100Reducer
	extends Reducer<NullWritable,Text,Text,Text> {

		Queue<RankNode> rankqueue = new PriorityQueue<>();

		public void reduce(NullWritable key, Iterable<Text> values,
				Context context
				) throws IOException, InterruptedException {
			for(Text value : values) {
				String[] parts = value.toString().split("``");
				rankqueue.offer(new RankNode(parts[0], Double.parseDouble(parts[1])));
			}
			
			int count = 99;
			while(count>=0 && !rankqueue.isEmpty()){
				RankNode node = rankqueue.poll();
				context.write(new Text(node.nodeid), new Text(node.pageRank+""));
				count--;
			}
		}
	}



	public static void main(String[] args) throws Exception {

		// Pre-Processing Job
		Configuration conf = new Configuration();
		conf.set("mapred.textoutputformat.separator", "``");
		System.setProperty("hadoop.home.dir", "/");
		Job job = Job.getInstance(conf, "pre-process records");
		FileInputFormat.addInputPath(job, new Path(args[0]));
		Path out = new Path(args[1] + "/pagerank_0/");
		FileOutputFormat.setOutputPath(job, out);
		out.getFileSystem(conf).delete(out, true);
		job.setJarByClass(PageRank.class);
		job.setMapperClass(PreProcessingMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.waitForCompletion(true);
		long totalnodes = job.getCounters().findCounter(PageRankCounters.TOTAL_NODES).getValue();
		conf.setLong("pagerank.totalnodes", totalnodes);



		// Page Rank 10 Iterations
		for(int iteration = 1; iteration <= 10 ; iteration++) {
			//Configuration conf3  = new Configuration();
			conf.set("mapred.textoutputformat.separator", "``");
			// set the depth into the configuration
			//conf.set("recursion.depth", depth + "");

			Job job1 = Job.getInstance(conf, "page rank");
			job1.setJobName("Page Raank Iteration-" + iteration);
			job1.setMapperClass(PageRankMapper.class);
			job1.setReducerClass(PageRankReducer.class);
			job1.setJarByClass(PageRank.class);
			Path in = new Path(args[1] + "/pagerank_" + (iteration - 1) + "/");
			Path out1 = new Path(args[1] + "/pagerank_" + iteration);
			FileInputFormat.addInputPath(job1, in);
			FileOutputFormat.setOutputPath(job1, out1);
			out1.getFileSystem(conf).delete(out1, true);
			job1.setMapOutputKeyClass(Text.class);
			job1.setMapOutputValueClass(Node.class);
			job1.setOutputKeyClass(Text.class);
			job1.setOutputValueClass(Text.class);
			job1.waitForCompletion(true);
			conf.setLong("pagerank.delta", job1.getCounters().findCounter(PageRankCounters.DANGLING_VALUE).getValue());
			job1.getCounters().findCounter(PageRankCounters.DANGLING_VALUE).setValue(0);
		}


		// Top-100 Job
		Job job2 = Job.getInstance(conf, "top-100 records");
		Path in = new Path(args[1] + "/pagerank_10");
		FileInputFormat.addInputPath(job2, in);
		Path outPath = new Path(args[1] + "/top100pagerank");
		FileOutputFormat.setOutputPath(job2, outPath);
		outPath.getFileSystem(conf).delete(outPath, true);
		job2.setJarByClass(PageRank.class);
		job2.setMapperClass(Top100Mapper.class);
		job2.setMapOutputKeyClass(NullWritable.class);
		job2.setMapOutputValueClass(Text.class);
		job2.setReducerClass(Top100Reducer.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);
		System.exit(job2.waitForCompletion(true) ? 0 : 1);


	}
}