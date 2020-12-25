
package wordFrequency;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * Sort the keys(relative frequencies) in decreasing order.
 */
public class KeyComparator extends WritableComparator {
	protected KeyComparator() {
		super(DoubleWritable.class, true);
    }
    
    @SuppressWarnings("rawtypes")
    @Override
    public int compare(WritableComparable a, WritableComparable b) {
    	DoubleWritable k1 = (DoubleWritable) a;
        DoubleWritable k2 = (DoubleWritable) b;          
        return -1 * k1.compareTo(k2);
    }
}
