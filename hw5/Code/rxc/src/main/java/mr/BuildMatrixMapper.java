package mr;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Set;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class BuildMatrixMapper
extends Mapper<Object, Text, Text, Text>{


	HashMap<String, Integer> indexMap = new HashMap<String, Integer>();

	// Load Disrtibuted FIle cache into map and read accordingly
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
				//System.out.println(sCurrentLine);
				indexMap.put(parts[0], Integer.parseInt(parts[1]));
			}
		}
	}


	public void map(Object key, Text value, Context context
			) throws IOException, InterruptedException {


		String line = value.toString();
		String parts[] = line.split("~~");
		// convert all the nodes into sparse form accordingly and write them down
		String sparseadjlist = "";
		if(!parts[1].equals("[]")){
			String[] adjnodes = parts[1].substring(1, parts[1].length()-1).split(", ");
			//System.out.println(adjnodes.length);
			float contrib = (float)1/adjnodes.length;
			for(String adjnode : adjnodes) {
				sparseadjlist += "(" + indexMap.get(adjnode) + ":"+ contrib + ")" + "##";
			}
			sparseadjlist = sparseadjlist.substring(0, sparseadjlist.length()-2);
			//System.out.println(sparseadjlist);
		}
		else{
			//sparseadjlist += "";
		}


		context.write(new Text(parts[0]), new Text(sparseadjlist));
	}
}