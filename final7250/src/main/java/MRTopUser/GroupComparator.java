package MRTopUser;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;


public class GroupComparator extends WritableComparator {

    protected GroupComparator() {
        super(CompositeKey.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        return -1 * ((CompositeKey) a).getRate().compareTo(((CompositeKey) b).getRate());
    }
}
