import java.io.IOException;
import org.apache.hadoop.conf.Configuration;                       // Provides access to configuration parameters
import org.apache.hadoop.fs.Path;                                  // Names a file or directory in a FileSystem
import org.apache.hadoop.io.IntWritable;                           // A WritableComparable for ints
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;                                  // This class stores text using standard UTF8 encoding
import org.apache.hadoop.mapreduce.Job;                            // It allows the user to configure the job, submit it, control its execution, etc.
import org.apache.hadoop.mapreduce.Mapper;                         // Maps input key/value pairs to a set of intermediate key/value pairs
import org.apache.hadoop.mapreduce.Reducer;                        // Reduces a set of intermediate values which share a key to a smaller set of values
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;      // A base class for file-based InputFormats
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;    // A base class for OutputFormats that read from FileSystems
import org.apache.hadoop.util.GenericOptionsParser;

import org.json.JSONObject;                                        // For creating JSON objects and then performing operations
import org.json.JSONException;


public class QueryAuthor {
  
        public static class JsonMapper extends Mapper<Object, Text, Text, Text> {

                public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			
						Configuration conf = context.getConfiguration();	// Return configuration for this job
						String authorName = conf.get("qAuthor");			// Get the value associated with 'qAuthor'
                        try {
                                String line = value.toString();				// Convert text back to string
                                line = line.trim();							// It removes any leading and trailing whitespace from string
                                JSONObject jObject = new JSONObject(line);		// Create json object from the source json string
                                String a = jObject.getString("author");			// Returns the string associated with a key author
                                String b = jObject.getString("book");			// Returns the string associated with a key book
                                
								if (a.compareTo(authorName) == 0) {			// Compare the author, which user input's, with the author in every string
									String c = "{\"book\":\"" + b + "\"}";		// Concatenate strings and make a pair {"book":"<book name>"}
                                	context.write(new Text(a), new Text(c));	// Output key as author and string 'c' as value
								}
                        } catch (JSONException e) {}
                }
        }

        public static class JsonCombiner extends Reducer<Text, Text, Text, Text> {

                private static String st;	// Declared for making string containing all books

                public void reduce(Text author, Iterable<Text> books, Context context) throws IOException, InterruptedException {

                        int i = 1;		// It is declared as 1 for making string without comma

                        for (Text book : books) {			// Iterate over all the text values in books corresponding to a author 
                                if (i == 1) {				// This is used just to execute it once and take first value as it is without placing comma
                                        st = book.toString();		// Stores first string value in st  
                                        i = i + 1;			// Increment i so that this block not executed again
                                }
                                else {
                                        st = book.toString() + "," + st;	// Concatenate all the values present in 'books' with comma and previous values 
                                }
                        }
                        context.write(author, new Text(st));

                 }
        }


        public static class JsonReducer extends Reducer<Text, Text, Text, NullWritable> {

                private static String st;		// For making the complete string
                private static final String s1 = "{\"author\":\"";	/* Some final string varaibles used to construct the complete string in the end */
                private static final String s2 = "\", \"books\":[";
                private static final String s3 = "]}";

                public void reduce(Text author, Iterable<Text> books, Context context) throws IOException, InterruptedException {

                        int i = 1;		// It is declared as 1 for making string without comma
                        for (Text book : books){				// Iterate over all the text values in books corresponding to a author
                                if (i == 1) {					// This is used just to execute it once and take first value as it is without comma
                                        st = book.toString();			// Stores first string value in st
                                        i = i + 1;				// Increment i so that this block not executed again
                                }
                                else {
                                        st = book.toString() + "," + st;	// Concatenate all values present in 'books' with comma and previous values
                                }
                        }

                        String sfinal = s1 + author + s2 + st + s3;			// Make final string using the defined patterns in s1, s2, and s3
                        context.write(new Text(sfinal), NullWritable.get());		// Writes the output to the output directory with value as null
                }
        }


        public static void main(String[] args) throws Exception {

                Configuration conf = new Configuration();		// Creates new configuration for resources
                String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();		// Parse command line arguments generically
                if (otherArgs.length < 3) {
                        System.err.println("Usage: QueryAuthor <in> <out> <author>");
                        System.exit(2);
                }
		
				String author = otherArgs[2];		// Store the author input by the user
				conf.set("qAuthor", author);		// Set the author in the name 'qAuthor' for this configuration 
		
                Job job = new Job(conf, "QueryAuthor");			// Creates a new map-reduce job named CombineBooks 
                job.setJarByClass(QueryAuthor.class);			// Set jar using the defined class
                job.setMapperClass(JsonMapper.class);			// Set mapper class for the job
                job.setCombinerClass(JsonCombiner.class);		// Set combiner class for the job
                job.setReducerClass(JsonReducer.class);			// Set reducer class for job

                job.setMapOutputKeyClass(Text.class);			// Set mapper output key type
                job.setMapOutputValueClass(Text.class);			// Set mapper output value type

                job.setOutputKeyClass(Text.class);			// Set both (in that case only reducer) output key class
                job.setOutputValueClass(NullWritable.class);		// Set both (in that case only reducer) output value class

                FileInputFormat.addInputPath(job, new Path(otherArgs[0]));		// Set the path for input directory
                FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));		// Set the path for output directory

                System.exit(job.waitForCompletion(true) ? 0 : 1);	// Submit the job to the hadoop and wait for its completion
        }

}

