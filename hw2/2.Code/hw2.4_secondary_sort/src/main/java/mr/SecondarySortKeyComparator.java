package mr;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class SecondarySortKeyComparator extends WritableComparator{

	public SecondarySortKeyComparator(){
		super(CompositeKeyWritable.class, true);
	}

	/* Key Comparator compares the station id and then if the objects
	 * belong to same station, then it compares the year value of the 
	 * 
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

		// Compare stationid of two object
		int result = keyobj1.getStationId().compareTo(keyobj2.getStationId());

		// only if both stationids are same, it returns compareto of first and second
		// objects year fields
		if(result == 0) {
			result = keyobj1.getYear().compareTo(keyobj2.getYear());
		}

		return result;
	}
}
