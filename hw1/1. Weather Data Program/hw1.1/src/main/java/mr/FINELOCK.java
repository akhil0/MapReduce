package mr;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.collections4.ListUtils;

class FINELOCKRunnable implements Runnable {

	List<String> listo = new ArrayList<String>();

	FINELOCKRunnable (List<String> listop) {
		listo = listop;
	}

	public void run() {
		
		listo.forEach(n -> {
			
			String[] parts = n.split(",");
			if(parts[2].equals("TMAX")) {
			ValObj tempobj = FINELOCK.tempmap.get(parts[0]);
			
			// Refer to report for clear explanation regarding this method.
			// If two threads access it for first time, first thread gets to put the value and second
			// thread will wait for the first thread to finish and then continue working on it.
			if(tempobj != null) {
				updateobj(parts[3],tempobj);
			}
			else {
				tempobj = FINELOCK.tempmap.putIfAbsent(parts[0],new ValObj(Double.parseDouble(parts[3]), 1));
				if(tempobj!= null) {
					updateobj(parts[3],tempobj);
				}
			}
			}
		});
	}

	public ValObj updateobj(String tmaxval, ValObj obj){
		//synchronized block for updateobj
		synchronized(obj){
			obj.total = obj.total + Double.parseDouble(tmaxval);
			obj.count++;
		}
		return obj;
	}
}


public class FINELOCK {

	static ConcurrentHashMap<String, ValObj> tempmap = new ConcurrentHashMap<String,ValObj>();

	public long FineLockRun(String filepath) throws InterruptedException, IOException {

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
			threadlist[i] = new Thread(new FINELOCKRunnable(output.get(i)));
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
