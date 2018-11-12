package RunTimeArguments;
import java.io.IOException; 
import java.util.StringTokenizer;
import org.apache.hadoop.io.*;  		  
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.Reducer.Context; 

public class WCMRv2Mapper extends Mapper<Object, Text, Text, IntWritable>{

	private final static IntWritable one = new IntWritable(1);
	private Text word = new Text();
	//int lower;
	//int upper;
	
	//set up method is called only once after JVM created
	public void setup(Context context)throws IOException, InterruptedException{
		//lower = Integer.parseInt(context.getConfiguration().get("LowerLimit"));
		//upper = Integer.parseInt(context.getConfiguration().get("UpperLimit"));
	}

	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		String line = value.toString();
		StringTokenizer tokenizer = new StringTokenizer(line);
		while (tokenizer.hasMoreTokens()) {
			word.set(tokenizer.nextToken());
			context.write(word, one);
		}
	}
	
	// cleanup method is called only once before JVM exit
	public void cleanup(Context context)throws IOException,InterruptedException{
		
	}
}
