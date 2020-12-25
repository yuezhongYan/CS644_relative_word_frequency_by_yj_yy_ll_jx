
package wordFrequency;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.Date;

public class RelativeWordFrequencyTest {

    public static void main(String[] args
    		) throws IOException, InterruptedException, ClassNotFoundException {

    	/* 
    	 * Job 1: 
    	 * This job obtains all combinations of all the words occurring together.
    	 */
        Job job1 = Job.getInstance(new Configuration());
        job1.setJarByClass(RelativeWordFrequencyTest.class);
        job1.setJobName("RelativeWordFrequency");

        job1.setMapperClass(LowerCaseWordPairMapper.class);
        job1.setReducerClass(RelativeWordCountReducer.class);
        job1.setCombinerClass(WordCombiner.class);
        job1.setPartitionerClass(WordPartitioner.class);
        job1.setNumReduceTasks(2);
        
        /*
         * The output file stores <word, relative frequency> pairs.
         * Create the name of the output file for being used as input for the next job.
         */
        String relativeFrequencyOutputFile = "relative-frequency-output-file";
        
        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(relativeFrequencyOutputFile));

        job1.setOutputKeyClass(WordWritable.class);
        job1.setOutputValueClass(IntWritable.class);
        job1.waitForCompletion(true);

        /* 
         * Job 2:
         * This job takes the output folder from job 1 as an input and obtains the final
         * result using the user-input file name.
         */
        Job job2 = Job.getInstance(new Configuration());
        job2.setJarByClass(RelativeWordFrequencyTest.class);
        job2.setJobName("RelativeWordFrequency2");
        job2.setMapperClass(wordPairRelativeFrequencyMapper.class);
        job2.setReducerClass(TopWordsReducer.class);
        job2.setNumReduceTasks(1);
        job2.setOutputKeyClass(DoubleWritable.class);
        job2.setOutputValueClass(WordWritable.class);
        job2.setSortComparatorClass(KeyComparator.class);
        FileInputFormat.addInputPath(job2, new Path(relativeFrequencyOutputFile));
        FileOutputFormat.setOutputPath(job2, new Path(args[1]));
        System.exit(job2.waitForCompletion(true) ? 0 : 1);
    }
}
