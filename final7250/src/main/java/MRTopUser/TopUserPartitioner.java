package MRTopUser;


import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class TopUserPartitioner extends Partitioner<CompositeKey, LongWritable> {

	@Override
    public int getPartition(CompositeKey compositeKey, LongWritable longWritable, int numPartitions) {
        return compositeKey.getRate().hashCode() % numPartitions;
    }

	

}
