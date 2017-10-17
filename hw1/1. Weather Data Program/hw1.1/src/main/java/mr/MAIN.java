package mr;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class MAIN {

	public static void main(String[] args) throws IOException, InterruptedException {
		//String path = "/Users/akhil0/Documents/workspace/hw1.1/src/1912.csv.gz";
		
		String path = args[0];
		
//		System.out.println(new FINELOCK().FineLockRun());
//		System.out.println(new COARSELOCK().CoarseLockRun());
//		System.exit(0);
		
		//SEQ Time Calculations
		List<Long> seqlist = new ArrayList<Long>();

		for(int i = 0; i < 10 ; i++) {
			long val = new SEQ().SEQRun(path);
			seqlist.add(val);
		}

		System.out.println("MAX of SEQ Run : " + Collections.max(seqlist));
		System.out.println("MIN of SEQ Run : " + Collections.min(seqlist));
		System.out.println("AVG of SEQ Run : " + avgof(seqlist));
		
		
		//SEQFib Time Calculations
		List<Long> seqfiblist = new ArrayList<Long>();

		for(int i = 0; i < 10 ; i++) {
			long val = new SEQFib().SEQRunFib(path);
			seqfiblist.add(val);
		}

		System.out.println("MAX of SEQFib Run : " + Collections.max(seqfiblist));
		System.out.println("MIN of SEQFib Run : " + Collections.min(seqfiblist));
		System.out.println("AVG of SEQFib Run : " + avgof(seqfiblist));

		//NOLOCK Time Calculations
		List<Long> nolocklist = new ArrayList<Long>();

		for(int i = 0; i < 10 ; i++) {
			long val = new NOLOCK().NoLockRun(path);
			nolocklist.add(val);
		}

		System.out.println("MAX of NOLOCK Run :" + Collections.max(nolocklist));
		System.out.println("MIN of NOLOCK Run : " + Collections.min(nolocklist));
		System.out.println("AVG of NOLOCK Run : " + avgof(nolocklist));

		//NOLOCKFib Time Calculations
		List<Long> nolockfiblist = new ArrayList<Long>();

		for(int i = 0; i < 10 ; i++) {
			long val = new NOLOCKFib().NoLockRunFib(path);
			nolockfiblist.add(val);
		}

		System.out.println("MAX of NOLOCKFib Run : " + Collections.max(nolockfiblist));
		System.out.println("MIN of NOLOCKFib Run : " + Collections.min(nolockfiblist));
		System.out.println("AVG of NOLOCKFib Run : " + avgof(nolockfiblist));

		//FINELOCK Time Calculations
		List<Long> finelist = new ArrayList<Long>();

		for(int i = 0; i < 10 ; i++) {
			long val = new FINELOCK().FineLockRun(path);
			finelist.add(val);
		}

		System.out.println("MAX of FINELOCK Run : " + Collections.max(finelist));
		System.out.println("MIN of FINELOCK Run : " + Collections.min(finelist));
		System.out.println("AVG of FINELOCK Run : " + avgof(finelist));

		//FINELOCKFib Time Calculations
		List<Long> finefiblist = new ArrayList<Long>();

		for(int i = 0; i < 10 ; i++) {
			long val = new FINELOCKFib().FineLockFibRun(path);
			finefiblist.add(val);
		}

		System.out.println("MAX of FINELOCKFib Run : " + Collections.max(finefiblist));
		System.out.println("MIN of FINELOCKFib Run : " + Collections.min(finefiblist));
		System.out.println("AVG of FINELOCKFib Run : " + avgof(finefiblist));

		//COARSELOCK Time Calculations
		List<Long> coarselist = new ArrayList<Long>();

		for(int i = 0; i < 10 ; i++) {
			long val = new COARSELOCK().CoarseLockRun(path);
			coarselist.add(val);
		}

		System.out.println("MAX of COARSELOCK Run : " + Collections.max(coarselist));
		System.out.println("MIN of COARSELOCK Run : " + Collections.min(coarselist));
		System.out.println("AVG of COARSELOCK Run : " + avgof(coarselist));
		//COARSELOCK Time Calculations
		List<Long> coarsefiblist = new ArrayList<Long>();

		for(int i = 0; i < 10 ; i++) {
			long val = new COARSELOCKFib().CoarseLockFibRun(path);
			coarsefiblist.add(val);
		}

		System.out.println("MAX of COARSELOCKFib Run : " + Collections.max(coarsefiblist));
		System.out.println("MIN of COARSELOCKFib Run : " + Collections.min(coarsefiblist));
		System.out.println("AVG of COARSELOCKFib Run : " + avgof(coarsefiblist));


		//NOSHARING Time Calculations
		List<Long> nosharelist = new ArrayList<Long>();

		for(int i = 0; i < 10 ; i++) {
			long val = new NOSHARING().NoSharingRun(path);
			nosharelist.add(val);
		}

		System.out.println("MAX of NOSHARING Run : " + Collections.max(nosharelist));
		System.out.println("MIN of NOSHARING Run : " + Collections.min(nosharelist));
		System.out.println("AVG of NOSHARING Run : " + avgof(nosharelist));

		//NOSHARINGFib Time Calculations
		List<Long> nosharefiblist = new ArrayList<Long>();

		for(int i = 0; i < 10 ; i++) {
			long val = new NOSHARINGFib().NoSharingRunFib(path);
			nosharefiblist.add(val);
		}

		System.out.println("MAX of NOSHARINGFib Run : " + Collections.max(nosharefiblist));
		System.out.println("MIN of NOSHARINGFib Run : " + Collections.min(nosharefiblist));
		System.out.println("AVG of NOSHARINGFib Run : " + avgof(nosharefiblist));
	}

	private static float avgof(List<Long> somelist) {
		long total = 0;
		for(long a : somelist){
			total += a;
		}
		float avg = total/somelist.size();
		return avg;
	}


}
