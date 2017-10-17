package mr;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

public class SEQFib {

	public long SEQRunFib(String filepath) throws IOException {

		String path = filepath;
		ReadFile read = new ReadFile(path);
		ArrayList<String> al = read.LoadFile();

		HashMap<String,ValObj> ValueMap = new HashMap<String,ValObj>();

		long starttime = System.currentTimeMillis();

		//Going through the String list to extract station ids and TMAXs
		al.forEach(n -> {
			String[] parts = n.split(",");
			if(parts[2].equals("TMAX")) {
				if(ValueMap.containsKey(parts[0])){
					ValObj TempObj = ValueMap.get(parts[0]);
					Fib fibo = new Fib();
					fibo.calfib(17);
					TempObj.total = TempObj.total + Double.parseDouble(parts[3]);
					TempObj.count++;
					ValueMap.put(parts[0], TempObj);
				}
				else {
					ValObj tempobj = new ValObj(Double.parseDouble(parts[3]), 1);
					ValueMap.put(parts[0], tempobj);
				}
			}
		});


		long endtime = System.currentTimeMillis();
		return (endtime-starttime);

	}
}
