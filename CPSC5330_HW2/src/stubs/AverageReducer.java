package stubs;
import java.io.IOException;

//import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class AverageReducer extends Reducer<Text, IntWritable, Text, Text> {

  @Override
  public void reduce(Text key, Iterable<IntWritable> values, Context context)
      throws IOException, InterruptedException {

    /*
     * 1) Sum of Reviews
     * 2) Average Rating
     */
	  long sum = 0, count = 0;
	  
	  /*
	   * For each value in the set of values passed to use by the mapper:
	   */
	  for (IntWritable value : values) {
		  /*
		   * Add up the values and increment the count
		   */
		  // produt id [rating 1,2,3,4]
		  // count over value
		  sum += value.get();
		  count++;
	  }
	  
	  if (count != 0) {
		  /*
		   * the average length is the sum of the values divided by the count
		   */
		  double result = (double)sum / (double) count;
		  
		  Text output = new  Text(count + " " + result); 
		  
		  /*
		   * call the write method on the Context object to emit a key 
		   * (the words' starting letter) and a value (the average length per word
		   * starting with this letter) from the reduce method.
		   */
		  context.write(key, output);
	  }

  }
}