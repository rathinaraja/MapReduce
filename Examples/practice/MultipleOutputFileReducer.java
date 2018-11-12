package practice;

import java.io.IOException; 
import org.apache.hadoop.io.*;  		  
import org.apache.hadoop.mapreduce.*; 
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

// instead of using context to write we assign context to multipleOutputs object

public class  MultipleOutputFileReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
	private IntWritable result = new IntWritable();
	private MultipleOutputs<Text,IntWritable> multipleOutputs;

	public void setup(Context context) throws IOException, InterruptedException{
		multipleOutputs=new MultipleOutputs<Text,IntWritable>(context);
	} 
	
	public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
		int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			multipleOutputs.write(key, result,key.toString().substring(0,1)); 
			// the third argument is filename, here key that that starts with same letter is put into same file
	}
	
	public void cleanup(Context context)throws IOException, InterruptedException{
		multipleOutputs.close();
	}
}

