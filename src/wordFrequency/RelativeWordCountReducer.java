
package wordFrequency;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class RelativeWordCountReducer
	extends Reducer<WordWritable, IntWritable, WordWritable, DoubleWritable> {
	/**
	 * Relative word count. Used when the next word is not a star.
	 */
	private DoubleWritable relativeWordCount = new DoubleWritable();
	
	/**
	 * Total word count based on the sum-up frequency.
	 */
	private DoubleWritable totalWordCount = new DoubleWritable();
	
	/**
	 * Initalize the current word. May be changed.
	 */
    private Text currentWord = new Text("NULL_VALUE");
    
    /**
     * Flag for star.
     */
    private Text flag = new Text("*");

    @Override
    protected void reduce(WordWritable key, Iterable<IntWritable> values, Context context
    		) throws IOException, InterruptedException {
        if (key.getNextWord().equals(flag)) {
            if (key.getCurrentWord().equals(currentWord)) {
            	totalWordCount.set(totalWordCount.get() + obtainIntSum(values));
            }else{
                currentWord.set(key.getCurrentWord());
                totalWordCount.set(0);
                totalWordCount.set(obtainIntSum(values));
            }
        }else{
        	int wordCount = obtainIntSum(values);
            relativeWordCount.set((double) wordCount / totalWordCount.get());
            context.write(key, relativeWordCount);
        }

    }

    /**
     * Method to sum up frequency.
     * 
     * @param values Iterable<IntWritable>
     * @return sum-up frequency
     */
    private int obtainIntSum(Iterable<IntWritable> values) {
        int sum = 0;
        for (IntWritable value : values) {
        	sum += value.get();
        }
        return sum;
    }
}
