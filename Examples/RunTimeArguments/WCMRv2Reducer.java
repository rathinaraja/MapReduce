package RunTimeArguments;
import java.io.IOException; 
import org.apache.hadoop.io.*;  		  
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.Mapper.Context; 

public class WCMRv2Reducer extends Reducer<Text,IntWritable,Text,IntWritable> {
	private IntWritable result = new IntWritable();
	int lower;
	int upper;
	
	//set up method is called only once after JVM created
	public void setup(Context context)throws IOException, InterruptedException{
		lower = Integer.parseInt(context.getConfiguration().get("LowerLimit"));
		upper = Integer.parseInt(context.getConfiguration().get("UpperLimit"));
	}
	
	public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
		int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			if(sum>=lower && sum<=upper){
				result.set(sum);
				context.write(key, result);
			}			
	}
	
	// cleanup method is called only once before JVM exit
	public void cleanup(Context context)throws IOException,InterruptedException{
		
	}
}
