package mr;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;



public class Top100Reducer 
extends Reducer<RankNode,DoubleWritable,NullWritable,Text> {

	HashMap<Integer, String> reverseIndexMap = new HashMap<Integer, String>();

	// Load Distributed File Cache into HashMap for reverse lookup
	protected void setup(Context context)
			throws IOException, InterruptedException { 
		URI[] uri = context.getCacheFiles();
		for (URI uriElement : uri) {
			//  System.out.println(uriElement);
			//System.exit(0);
			// Load all files from path
			Path awsPath = new Path("s3://mr.hw5/"+uriElement.getPath());
			FileSystem fs = FileSystem.get(awsPath.toUri(), context.getConfiguration());
			BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(awsPath)));
			String sCurrentLine;
			while ((sCurrentLine = reader.readLine()) != null) {
				String[] parts = sCurrentLine.split("~~");
				reverseIndexMap.put(Integer.parseInt(parts[1]), parts[0]);
			}
		}
	}



	public void reduce(RankNode key, Iterable<DoubleWritable> values,
			Context context
			) throws IOException, InterruptedException {

		int i = 0;
		for(DoubleWritable value : values){
			// print 100 files in reverse order
			int pgName = Integer.parseInt(key.nodeid);
			context.write(NullWritable.get(), new Text(reverseIndexMap.get(pgName)+"~~" +value.get()));
			i++;
			if(i == 100) break;
		}
	}
}