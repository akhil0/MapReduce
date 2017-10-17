package mr;

import java.io.IOException;
import java.util.HashMap;
import java.util.Set;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;



public class IndexMapper
extends Mapper<Object, Text, Text, Text>{

	HashMap<String, String> nodeMap = new 
			HashMap<String, String>();
	// ADd nodes into hashmap for in-mapper combiner and have index id
	public void map(Object key, Text value, Context context
			) throws IOException, InterruptedException {
		String line = value.toString();
		String parts[] = line.split("~~");
		if(!nodeMap.containsKey(parts[0]))
			nodeMap.put(parts[0], null);
		if(!parts[1].equals("[]")){
			String adjline = parts[1].substring(1, parts[1].length()-1).trim();
			String[] adjnodelist = adjline.split(", ");
			for(String adjnode : adjnodelist){
				if(!nodeMap.containsKey(adjnode))
					nodeMap.put(adjnode, null);
			}
		}
	}

	// emit out all the nodes in map
	public void cleanup(Context context)
			throws IOException, InterruptedException {
		Set<String> keyset = nodeMap.keySet();
		for(String s : keyset) {
			context.write(new Text("1"), new Text(s));
		}
	}
}