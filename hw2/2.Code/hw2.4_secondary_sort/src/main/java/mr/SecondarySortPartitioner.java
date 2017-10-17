package mr;

import org.apache.hadoop.mapreduce.Partitioner;

public class SecondarySortPartitioner extends
Partitioner<CompositeKeyWritable, CompositeValueWritable> {

	@Override
	public int getPartition(CompositeKeyWritable key, CompositeValueWritable value,
			int numReduceTasks) {

		// divide the each reduce call based on composite key.stationid hashed and
		// modded with no. of reducers
		// We can set no of reducers from job when starting.
		return Math.abs(key.getStationId().hashCode() % numReduceTasks);
	}
}