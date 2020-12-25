
package wordFrequency;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class WordPartitioner extends Partitioner<WordWritable, IntWritable> {
    @Override
    public int getPartition(WordWritable key, IntWritable count, int numReduceTasks) {
        return (key.getCurrentWord().hashCode() & Integer.MAX_VALUE ) % numReduceTasks;
    }
}
