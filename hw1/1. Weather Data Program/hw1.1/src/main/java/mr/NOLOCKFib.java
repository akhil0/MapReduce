package mr;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.commons.collections4.ListUtils;

class NOLOCKRunnableFib implements Runnable {

	List<String> listo = new ArrayList<String>();

	NOLOCKRunnableFib (List<String> listop) {
		listo = listop;
	}

	public void run() {
		listo.forEach(n -> {
			String[] parts = n.split(",");
			if(parts[2].equals("TMAX")) {
			//System.out.println(parts[0] + " : " + parts[3]);
			if(NOLOCKFib.SharedMap.containsKey(parts[0])){
				ValObj tempobj = NOLOCKFib.SharedMap.get(parts[0]);
				Fib fibo = new Fib();
				fibo.calfib(17);
				tempobj.total = tempobj.total + Double.parseDouble(parts[3]);
				tempobj.count++;
				NOLOCKFib.SharedMap.put(parts[0], tempobj);
			}
			else {
				ValObj tempobj = new ValObj(Double.parseDouble(parts[3]), 1);
				NOLOCKFib.SharedMap.put(parts[0], tempobj);
			}
			}
		});
	}
}


public class NOLOCKFib {

	static HashMap<String,ValObj> SharedMap = new HashMap<String,ValObj>();
	
	public long NoLockRunFib(String filepath) throws IOException, InterruptedException {

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
			threadlist[i] = new Thread(new NOLOCKRunnableFib(output.get(i)));
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
