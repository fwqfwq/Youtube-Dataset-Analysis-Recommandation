package MRCategoriesRanking;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;


class SortKeyComparator extends WritableComparator {

    protected SortKeyComparator() {
        super(FloatWritable.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
    	FloatWritable key1 = (FloatWritable) a;
    	FloatWritable key2 = (FloatWritable) b;

        int result = key1.get() < key2.get() ? 1 : key1.get() == key2.get() ? 0 : -1;
        return result;
    }

}
