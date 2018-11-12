package practice; 

import java.io.IOException; 
import java.util.StringTokenizer;
import org.apache.hadoop.io.*;  		  
import org.apache.hadoop.mapreduce.*; 

public class MultipleOutputFileMapper extends Mapper<Object, Text, Text, IntWritable>{

	private final static IntWritable one = new IntWritable(1);
	private Text word = new Text();

	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		String line = value.toString();
		StringTokenizer tokenizer = new StringTokenizer(line);
		while (tokenizer.hasMoreTokens()) {
			// to covert everything into lowercase
			word.set(tokenizer.nextToken().toLowerCase());
			// check word starts with alphabet or not. if not alphabet then dont write it
			if(Character.isAlphabetic(word.toString().charAt(0)))			
				context.write(word, one);
			//you can preprocess data like this in mapper class
		}
	}
}
