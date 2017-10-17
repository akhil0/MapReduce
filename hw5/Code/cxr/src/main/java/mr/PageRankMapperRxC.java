package mr;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Set;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

import mr.PageRankDriverRxC.PageRankCounters;

public class PageRankMapperRxC extends Mapper<Object, Text, IntWritable, DoubleWritable>{

	HashMap<Integer, Double> rankMap = new HashMap<Integer, Double>();

	// Load Distributed File cache into hashmap for look up pagerank for that node.
	protected void setup(Context context)
			throws IOException, InterruptedException { 
		  URI[] uri = context.getCacheFiles();
		  for (URI uriElement : uri) {
			//  System.out.println(uriElement);
			  //System.exit(0);
			 Path awsPath = new Path("s3://mr.hw5/"+uriElement.getPath());
          	FileSystem fs = FileSystem.get(awsPath.toUri(), context.getConfiguration());
          	BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(awsPath)));
			String sCurrentLine;
			while ((sCurrentLine = reader.readLine()) != null) {
				String[] parts = sCurrentLine.split("~~");
				rankMap.put(Integer.parseInt(parts[0]), Double.parseDouble(parts[1]));
			}
		}
	}


	public void map(Object key, Text value, Context context
			) throws IOException, InterruptedException {

		String line = value.toString();
		String parts[] = line.split("~~");
		int nodeid = Integer.parseInt(parts[0]);
		//double pagerankcontrib = rankMap.get(nodeid);
		
		// if dangling node, add to counter
		if(parts.length < 2){
			long pagerank = (long) (Math.pow(10, 8) * rankMap.get(nodeid));
			//System.out.println(pagerank);
			context.getCounter(PageRankCounters.DANGLING_VALUE).increment(pagerank);
		}
		// for each nodeid, since we are doing Row x Column , each adjacency nodes of single row, gets multiplied by pagerank of that 
		// particular rowid and emit column id, pagerank
		else {
			String[] adjnodes = parts[1].split("##");
			for(String adjnode : adjnodes) {
				adjnode = adjnode.substring(1, adjnode.length()-1);
				String newparts[] = adjnode.split(":");
				int nid = Integer.parseInt(newparts[0]);
				double pagerankcontrib = rankMap.get(nid);
				double pagerank = Double.parseDouble(newparts[1])*pagerankcontrib;
				context.write(new IntWritable(nid), new DoubleWritable(pagerank));
			}
		}
	}
}