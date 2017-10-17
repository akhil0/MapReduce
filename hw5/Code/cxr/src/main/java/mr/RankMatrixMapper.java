package mr;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class RankMatrixMapper
extends Mapper<Object, Text, Text, Text>{



	public void map(Object key, Text value, Context context
			) throws IOException, InterruptedException {
		
		
		String line = value.toString();
		String parts[] = line.split("~~");
		//System.out.println(parts[0] + " === "+ parts[1]);
		// initialize all the nodes in the index to 1/v as default.
		String nodeid = parts[0];
		double pagerank = (double)1/context.getConfiguration().getLong("matrix.pagerank.totalnodes",0);
		
		context.write(new Text(nodeid), new Text(pagerank+""));
	}
}