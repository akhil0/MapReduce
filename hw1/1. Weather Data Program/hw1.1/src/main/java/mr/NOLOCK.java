package mr;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.commons.collections4.ListUtils;

class NOLOCKRunnable implements Runnable {

	List<String> listo = new ArrayList<String>();

	NOLOCKRunnable (List<String> listop) {
		listo = listop;
	}

	public void run() {
		listo.forEach(n -> {
			String[] parts = n.split(",");
			if(parts[2].equals("TMAX")) {
				
				//update map accordingly
				if(NOLOCK.SharedMap.containsKey(parts[0])){
					ValObj tempobj = NOLOCK.SharedMap.get(parts[0]);
					tempobj.total = tempobj.total + Double.parseDouble(parts[3]);
					tempobj.count++;
					NOLOCK.SharedMap.put(parts[0], tempobj);
				}
				else {
					ValObj tempobj = new ValObj(Double.parseDouble(parts[3]), 1);
					NOLOCK.SharedMap.put(parts[0], tempobj);
				}
			}	
		});
	}
}


public class NOLOCK {

	static HashMap<String,ValObj> SharedMap = new HashMap<String,ValObj>();

	public long NoLockRun(String filepath) throws IOException, InterruptedException {

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
			threadlist[i] = new Thread(new NOLOCKRunnable(output.get(i)));
			threadlist[i].start();
		}

		// Barrier for all threads
		for(int kl = 0; kl < output.size(); kl++){
			threadlist[kl].join();
		}

		long endtime = System.currentTimeMillis();
		//System.out.println("Final HashMap size = " + SharedMap.size());
		return (endtime-starttime);
	}
}
