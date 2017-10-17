package mr;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.commons.collections4.ListUtils;

class NOSHARINGRunnableFib implements Runnable {

	List<String> listo = new ArrayList<String>();

	HashMap<String, ValObj> newmap = new HashMap<String, ValObj>();

	NOSHARINGRunnableFib (List<String> listop) {
		listo.addAll(listop);
	}

	public void run() {

		listo.forEach(n -> {
			String[] parts = n.split(",");
			if(parts[2].equals("TMAX")) {
				//update each map fr its own thread
				if(newmap.containsKey(parts[0])){
					ValObj tempobj = newmap.get(parts[0]);
					Fib fibo = new Fib();
					fibo.calfib(17);
					tempobj.total = tempobj.total + Double.parseDouble(parts[3]);
					tempobj.count++;
					newmap.put(parts[0], tempobj);
				}
				else {
					ValObj tempobj = new ValObj(Double.parseDouble(parts[3]), 1);
					newmap.put(parts[0], tempobj);
				}
			}
		});
	}


}


public class NOSHARINGFib {

	public long NoSharingRunFib(String filepath) throws InterruptedException, IOException {

		// Read File to list
		String path = filepath;
		ReadFile read = new ReadFile(path);
		ArrayList<String> al = read.LoadFile();

		int cores = Runtime.getRuntime().availableProcessors();

		//Find the size of each partition list
		int parts = al.size()/cores + 1;

		//Divide into 8 lists of equal sizes
		List<List<String>> output = ListUtils.partition(al, parts);

		Thread[] threadlist = new Thread[cores];
		NOSHARINGRunnableFib[] runnablelist = new NOSHARINGRunnableFib[cores];
		long starttime = System.currentTimeMillis();

		for(int i = 0; i < output.size(); i++) {
			runnablelist[i]= new NOSHARINGRunnableFib(output.get(i));
			threadlist[i] = new Thread(runnablelist[i]);
			threadlist[i].start();
		}


		//Barrier for all threads.
		for(int l = 0; l < output.size(); l++){
			threadlist[l].join();
		}

		HashMap<String, ValObj> CombinedMap = new HashMap<String, ValObj>();

		// Combines each data structure of each thread into single data structure.
		for(NOSHARINGRunnableFib r : runnablelist){
			r.newmap.forEach((k,v) -> {
				if(CombinedMap.containsKey(k)) {
					ValObj tempobj = CombinedMap.get(k);
					tempobj.total = tempobj.total + v.total;
					tempobj.count = tempobj.count + v.count;
					//CombinedMap.put(k, tempobj);
				}
				else {
					CombinedMap.put(k,v);
				}
			});
		}

		long endtime = System.currentTimeMillis();
		return (endtime-starttime);
	}

}