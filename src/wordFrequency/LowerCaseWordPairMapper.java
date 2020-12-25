
package wordFrequency;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class LowerCaseWordPairMapper 
	extends Mapper<LongWritable, Text, WordWritable, IntWritable> {
	
	/**
     * Temporarily store the current word and the nearby word. Will be used as a key.
     */
    private WordWritable word = new WordWritable();
    
    /**
     * Retrieve words one by one, and Assign each word and/or each group of the
     * current and the nearby word with its frequency to be 1. Will be used as a
     * value.
     */
    private IntWritable ONE = new IntWritable(1);
    
    /**
     * Frequency of the current and the nearby words.
     */
    private IntWritable wordFrequency = new IntWritable();

    @Override
    protected void map(LongWritable key, Text value, Context context
    		) throws IOException, InterruptedException {
    	int relativeWordCount = 1;
        String[] wordStringArray = value.toString().split("\\s+");
        int wordStringArraySize = wordStringArray.length;
        if (wordStringArraySize > 1) {
            for (int i = 0; i < wordStringArraySize; i++) {
                if(isMatched(wordStringArray[i])) {
                    wordStringArray[i] = wordStringArray[i].replaceAll("\\W+", "");

                    if(wordStringArray[i].equals("")){
                        continue;
                    }

                    word.setCurrentWord(wordStringArray[i].toLowerCase());

                    /*
                     * Handle word frequency with start and end pointers.
                     * Finally, (end - start) will be the frequency.
                     */
                    int start = 0;
                    int end = i + relativeWordCount;
                    if(i > relativeWordCount){
                       start = i - relativeWordCount;
                    }
                    if(i + relativeWordCount >= wordStringArraySize){
                        end = wordStringArraySize - 1;
                    }

                    /*
                     * Write the nearby word with frequency being 1.
                     */
                    for (int j = start; j <= end; j++) {
                        if (j == i) {
                        	continue;
                        }
                        
                        if(isMatched(wordStringArray[j])) {
                          wordStringArray[j] = wordStringArray[j].replaceAll("\\W", "");
                          word.setNextWord(wordStringArray[j].toLowerCase());
                          context.write(word, ONE);
                        }
                    }

                    /* Copy star to the next word. */
                    word.setNextWord("*");
                    
                    wordFrequency.set(end - start);
                    context.write(word, wordFrequency);
                }
            } // end of outer for loop
        }
    }
    
    /**
     * Method to check if a word string consists only letters.
     * 
     * @param wordString word string in input file
     * @return true if a word string consists only letters, otherwise false
     */
    private boolean isMatched(String wordString) {
    	return wordString.matches("^[A-Za-z]+$");
    }
}
