package mr;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.zip.GZIPInputStream;

public class ReadFile {

	String path;

	public ReadFile (String path) { 
		this.path = path; 

	}

	public ArrayList<String> LoadFile() throws IOException {

		String FILEPATH  = path;

		// GZ input Stream reader
		FileInputStream fin = new FileInputStream(FILEPATH);
		
		InputStreamReader xover = new InputStreamReader(fin);
		BufferedReader is = new BufferedReader(xover);
		ArrayList<String> list = new ArrayList<String>();
		String line;
		while ((line = is.readLine()) != null) {
			list.add(line);

		}
		return list;
	}

}

// Custom Object class which stores total (sum of TMAX) and count (no of stations)
class ValObj { 
	double total; 
	int count;

	public ValObj (double total, int count) { 
		this.total = total; 
		this.count = count; 
	} 
}

// Fibonoacci class
class Fib{
	public int calfib(int s) {
		return (s<3? 1: calfib(s-1)+calfib(s-2));

	}
}
