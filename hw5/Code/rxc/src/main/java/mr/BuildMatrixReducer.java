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
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import mr.PageRankDriverRxC.PageRankCounters;




public class BuildMatrixReducer
extends Reducer<Text,Text,Text,Text> {

	HashMap<String, Integer> indexMap = new HashMap<String, Integer>();
// Load Distributed File Cache into map
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
				indexMap.put(parts[0], Integer.parseInt(parts[1]));
			}
		}
	}
	
	//  Convert all the nodes that are not present in the index as well
	public void reduce(Text key, Iterable<Text> values,
			Context context) throws IOException, InterruptedException {
		
		String val = "";
		for(Text sample : values) {
			val += val + sample.toString();
		}
		String nodeid = indexMap.get(key.toString()) + "";
		context.write(new Text(nodeid), new Text(val));
		indexMap.remove(key.toString());
	}
	
	// clean up map and emit all nodes with no adjacency nodes
	public void cleanup(Context context)
			throws IOException, InterruptedException {
		Set<String> keyset = indexMap.keySet();
		for(String s : keyset) {
			String nodeid = indexMap.get(s) + "";
			context.write(new Text(nodeid), new Text(""));
		}
		
	}
}