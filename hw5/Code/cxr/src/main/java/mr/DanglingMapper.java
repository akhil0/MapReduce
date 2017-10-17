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

public class DanglingMapper extends Mapper<Object, Text, IntWritable, DoubleWritable>{

	HashMap<Integer, Double> rankMap = new HashMap<Integer, Double>();

	// Load Distributed file cache into Map
	protected void setup(Context context)
			throws IOException, InterruptedException { 
		URI[] uri = context.getCacheFiles();
		for (URI uriElement : uri) {
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

		// Add the dangling loss across all nodes
		String line = value.toString();
		String parts[] = line.split("~~");
		int nodeid = Integer.parseInt(parts[0]);
		double danglingloss = (context.getConfiguration().getLong("pagerank.delta",0))/(double)(Math.pow(10,8));
		long totalnodes = context.getConfiguration().getLong("matrix.pagerank.totalnodes",0);
		double dampeningfactor = 0.15;
		// if node is presen, just add dangling loss to the pagerank
		if(rankMap.containsKey(nodeid)){
			double pagerank = dampeningfactor/totalnodes + (1-dampeningfactor)*(rankMap.get(nodeid) + danglingloss/totalnodes);
			context.write(new IntWritable(nodeid), new DoubleWritable(pagerank));
		}
		// just add the dangling loss alone
		else{
			double pagerank = dampeningfactor/totalnodes + (1-dampeningfactor)*(danglingloss/totalnodes);
			context.write(new IntWritable(nodeid), new DoubleWritable(pagerank));
		}

	}
}