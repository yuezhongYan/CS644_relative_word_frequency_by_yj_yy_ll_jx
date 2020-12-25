
package wordFrequency;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

public class wordPairRelativeFrequencyMapper 
	extends Mapper<Object, Text, DoubleWritable, WordWritable>{

	/**
	 * Store the current word and the next word.
	 */
    private String[] wordStringArray;
    
    /**
     * Store the word token which the iterator points to and which is split by tab.
     */
    private String[] wordTokens;
       
    /**
     * Relative frequency used as a key to write.
     */
    private DoubleWritable relativeFrequency = new DoubleWritable();
  
    @Override
    public void map(Object key, Text value, Context context
    		) throws IOException, InterruptedException {
    	StringTokenizer itr = new StringTokenizer(value.toString(), "\n");
    	while (itr.hasMoreTokens()) {
    		wordTokens = itr.nextToken().toString().split("\t");
        	wordStringArray = wordTokens[0].toString().split(" ");
        	WordWritable wordPair = 
        			new WordWritable(wordStringArray[0], wordStringArray[1]);
        	relativeFrequency.set(Double.parseDouble(wordTokens[1].trim()));
              
        	if(relativeFrequency == null) {
        		continue;
        	}
              
        	context.write(relativeFrequency, wordPair);
        }
    }
}
