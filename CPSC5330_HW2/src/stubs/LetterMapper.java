package stubs;
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class LetterMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
  @Override
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
	  String IGNORE = "star_rating";
	  String line = value.toString(); 	 
	  String[] lines = line.split("\\t");
	  String productId = lines[7];
	  if(!productId.equals(IGNORE)){
		  int rating = Integer.parseInt(String.valueOf(lines[7]));  
		  context.write(new Text(productId), new IntWritable(rating));
	  }
  }
}