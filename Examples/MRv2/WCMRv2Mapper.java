package MRv2;
import java.io.IOException; 
import java.util.StringTokenizer;
import org.apache.hadoop.io.*;  		  
import org.apache.hadoop.mapreduce.*; 

public class WCMRv2Mapper extends Mapper<Object, Text, Text, IntWritable>{

	private final static IntWritable one = new IntWritable(1);
	private Text word = new Text();

	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		String line = value.toString();
		StringTokenizer tokenizer = new StringTokenizer(line);
		while (tokenizer.hasMoreTokens()) {
			word.set(tokenizer.nextToken());
			context.write(word, one);
		}
	}
}
