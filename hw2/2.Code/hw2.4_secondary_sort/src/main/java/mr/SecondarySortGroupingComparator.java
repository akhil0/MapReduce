package mr;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class SecondarySortGroupingComparator extends WritableComparator{
	
	public SecondarySortGroupingComparator(){
		super(CompositeKeyWritable.class, true);
	}

	/* Grouping Comparator groups the composite key objects based on key.stationid
	 * It groups all the station ids together in the order 
	 */
	
	
	/* (non-Javadoc)
	 * @see org.apache.hadoop.io.WritableComparator#compare(org.apache.hadoop.io.WritableComparable, org.apache.hadoop.io.WritableComparable)
	 */
	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		// TODO Auto-generated method stub
		//return super.compare(a, b);
		
		CompositeKeyWritable keyobj1 = (CompositeKeyWritable) a;
		CompositeKeyWritable keyobj2 = (CompositeKeyWritable) b;
		
		return keyobj1.getStationId().compareTo(keyobj2.getStationId());
	}
}
