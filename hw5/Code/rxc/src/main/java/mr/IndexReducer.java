package mr;

import java.io.IOException;
import java.util.HashMap;
import java.util.Set;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import mr.PageRankDriverRxC.PageRankCounters;




public class IndexReducer
extends Reducer<Text,Text,Text,Text> {

	HashMap<String, Integer> indexMap = new 
			HashMap<String, Integer>();
	int counter = 0;
	
	
	public void reduce(Text key, Iterable<Text> values,
			Context context) throws IOException, InterruptedException {
		// add nodes that are not in the graph into index
		for(Text value : values) {
			if(!indexMap.containsKey(value)){
				indexMap.put(value.toString(), counter);
				counter++;
			}
		}
		// Counter for totalnode incremented
		context.getCounter(PageRankCounters.TOTAL_NODES).increment(counter);
		
		Set<String> keyset = indexMap.keySet();
		for(String s : keyset) {
			context.write(new Text(s), new Text(indexMap.get(s) + ""));
		}

	}
}