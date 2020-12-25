
package wordFrequency;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class TopWordsReducer
	extends Reducer<DoubleWritable, WordWritable, WordWritable, DoubleWritable> {
	/**
	 * The upper bound for the number of a single word and/or word groups to write.
	 */
	static final int TOP_WORD_PAIRS_COUNT_CAP = 100;
	
	/**
	 * Frequency for a word/word group.
	 */
	static final double WORD_FREQUENCY = 1.0;
	
	/**
	 * Counter for numbers of word groups to write.
	 */
	private int topWordGroupsCount = 0;
	
    @Override
    protected void reduce(DoubleWritable key,
    		Iterable<WordWritable> values, Context context
    		) throws IOException, InterruptedException {
    	for(WordWritable value : values) {
    		if(topWordGroupsCount >= TOP_WORD_PAIRS_COUNT_CAP) {
    			break;
    		}
           
    		if(key.get() == WORD_FREQUENCY) {
    			continue;
    		}
             
    		context.write(value, key);
    		topWordGroupsCount++;
        }
    }
}
