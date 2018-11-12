package ChainingMRjobs;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable; 
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper; 

public class Mapper3  extends Mapper<Text, IntWritable, Text, IntWritable>{
	public void map(Text key, IntWritable value, Context context) throws IOException, InterruptedException {
		String[] line = value.toString().split("\t"); 
		int count=Integer.parseInt(line[1]);		
		IntWritable val=new IntWritable(count+1); 
		context.write(key, val); 
	}
}