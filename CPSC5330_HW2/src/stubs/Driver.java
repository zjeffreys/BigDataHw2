package stubs;

import org.apache.hadoop.fs.Path;
//import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.conf.Configuration;


public class Driver {

  public static void main(String[] args) throws Exception {

    /*
     * Validate that two arguments were passed from the command line.
     */
    if (args.length != 2) {
      System.out.printf("Usage: Amazon <input dir> <output dir>\n");
      System.exit(-1);
    }
    
    /*
     * Instantiate a Job object for your job's configuration. 
     */
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "Average Word Length");

    
    /*
     * Specify the jar file that contains your driver, mapper, and reducer.
     * Hadoop will transfer this jar file to nodes in your cluster running 
     * mapper and reducer tasks.
     */
    job.setJarByClass(Driver.class);
    
 

    /*
     * TODO implement
     */
    /*
     * Specify the paths to the input and output data based on the command-line arguments
     */
    FileInputFormat.setInputPaths(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    
    /*
     * Specify the mapper and reducer class
     */
    job.setMapperClass(LetterMapper.class);
    job.setReducerClass(AverageReducer.class);
    
    /*
     * The mapper's output keys and values have different data types
     * than the reducer's output keys and values. Therefore, you must call
     * the setMapOutputKeyClass and setMapOutputValueClass methods.
     */
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(IntWritable.class);
    
    /*
     * Specify the job's output key and value classes.
     */
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    
    /*
     * Start the MapReduce job and wait for it to finish.
     * If it finishes successfully, return 0. If not, return 1.
     */
    boolean success = job.waitForCompletion(true);
    System.exit(success ? 0 : 1);
  }
}

