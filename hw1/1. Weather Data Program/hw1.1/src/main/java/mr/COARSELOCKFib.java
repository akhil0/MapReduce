package mr;


import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections4.ListUtils;

class COARSELOCKRunnableFib implements Runnable {

	List<String> listo = new ArrayList<String>();

	//Constructor for Runnable Class
	COARSELOCKRunnableFib (List<String> listop) {
		listo.addAll(listop);
	}

	public void run() {
		// work on each list to update shared data structure
		listo.forEach(n -> {
			String[] parts = n.split(",");
			if(parts[2].equals("TMAX")) {
				//System.out.println(parts[0] + " : " + parts[3]);
				COARSELOCKFib.updatemap(parts[0], parts[3]);
			}
		});
	}
}


public class COARSELOCKFib {

	static HashMap<String,ValObj> tempmap = new HashMap<String,ValObj>();

	// Synchronized method to update the hashmap with new sum & count for a key.
	public static synchronized HashMap<String, ValObj> updatemap(String skey, String svalue){
		if(tempmap.containsKey(skey)){
			ValObj tempobj = tempmap.get(skey);
			Fib fibo = new Fib();
			fibo.calfib(17);
			tempobj.total = tempobj.total + Double.parseDouble(svalue);
			tempobj.count++;
			tempmap.put(skey, tempobj);
		}
		else {
			ValObj tempobj = new ValObj(Double.parseDouble(svalue), 1);
			tempmap.put(skey, tempobj);
		}
		return tempmap;
	}

	static Map<String,ValObj> map = Collections.synchronizedMap(tempmap);

	public long CoarseLockFibRun(String filepath) throws InterruptedException, IOException {

		//Reading file input into List
		String path = filepath;
		ReadFile read = new ReadFile(path);
		ArrayList<String> al = read.LoadFile();

		int cores = Runtime.getRuntime().availableProcessors();

		//Finding list size in each thread
		int parts = al.size()/cores + 1;

		//Partitions list into 'core'(8) no. of lists with equal sizes
		List<List<String>> output = ListUtils.partition(al, parts);

		Thread[] threadlist = new Thread[output.size()];
		long starttime = System.currentTimeMillis();
		for(int i = 0; i < output.size(); i++) {
			threadlist[i] = new Thread(new COARSELOCKRunnableFib(output.get(i)));
			threadlist[i].start();
		}

		// Barrier for all threads
		for(int kl = 0; kl < output.size(); kl++){
			threadlist[kl].join();
		}

		long endtime = System.currentTimeMillis();
		return (endtime-starttime);
	}

}
