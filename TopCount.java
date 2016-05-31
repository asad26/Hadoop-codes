import java.io.IOException;       
import java.util.StringTokenizer;	// Allows an application to break a string into tokens

import org.apache.hadoop.conf.Configuration;	// Provides access to configuration parameters
import org.apache.hadoop.fs.Path;				// Names a file or directory in a FileSystem
import org.apache.hadoop.io.IntWritable;		// A WritableComparable for ints
import org.apache.hadoop.io.Text;				// This class stores text using standard UTF8 encoding
import org.apache.hadoop.mapreduce.Job;			// It allows the user to configure the job, submit it, control its execution, and query the state
import org.apache.hadoop.mapreduce.Mapper;		// Maps input key/value pairs to a set of intermediate key/value pairs
import org.apache.hadoop.mapreduce.Reducer;		// Reduces a set of intermediate values which share a key to a smaller set of values
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;		// A base class for file-based InputFormats
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;		// A base class for OutputFormats that read from FileSystems

public class TopCount {

	/* A mapper class which takes whole line as an input, then tokenize it, assign value 1 to each word and send to combiner phase
		for counting the word */
		
	public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable>{

    		private final static IntWritable one = new IntWritable(1);
    		private Text word = new Text();

    		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        		StringTokenizer itr = new StringTokenizer(value.toString());
      			while (itr.hasMoreTokens()) {
        			word.set(itr.nextToken());
        			context.write(word, one);
     		 	}
    		}
  	}	
	
	/* Combiner class for reducing key-value pairs. We cannot use the same reducer code as a combiner in that case.
		Becuase we cannot conclude at local level (during combiner phase) whether sum is >= 100 or not. Therefore separate class is needed */
	
	public static class IntSumCombiner extends Reducer<Text, IntWritable, Text, IntWritable> {

                private IntWritable result = new IntWritable();

                public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
                        int sum = 0;
                        for (IntWritable val : values) {
                                sum += val.get();
                        }
                        result.set(sum);
                        context.write(key, result);
                       
                }
        }

	/* This reducer code is exactly same except one difference is there. It would output key-value pair only and only if the sum would 
		>= 100 for a corresponding word (key) */
		
	public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    
		private IntWritable result = new IntWritable();

    		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
      			int sum = 0;
      			for (IntWritable val : values) {
        			sum += val.get();
      			}
      			if (sum >= 100) {		// This condition checks whether the word appears atleast 100 times or not, then produce the output.
					result.set(sum);
      				context.write(key, result);
			}
   	 	}		
  	}	

	public static void main(String[] args) throws Exception {
    		Configuration conf = new Configuration();
    		Job job = Job.getInstance(conf, "Top Count");
    		job.setJarByClass(TopCount.class);
    		job.setMapperClass(TokenizerMapper.class);
    		job.setCombinerClass(IntSumCombiner.class);
    		job.setReducerClass(IntSumReducer.class);
    		job.setOutputKeyClass(Text.class);
    		job.setOutputValueClass(IntWritable.class);
    		FileInputFormat.addInputPath(job, new Path(args[0]));
    		FileOutputFormat.setOutputPath(job, new Path(args[1]));
    		System.exit(job.waitForCompletion(true) ? 0 : 1);
  	}
}
