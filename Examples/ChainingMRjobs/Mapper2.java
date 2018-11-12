package ChainingMRjobs;

import java.io.IOException; 
import org.apache.hadoop.io.*;  		  
import org.apache.hadoop.mapreduce.*; 

public class Mapper2 extends Mapper<LongWritable, Text, Text, IntWritable>{
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] line = value.toString().split("\t"); 
		/*System.out.println(line[0]);
		System.out.println(line[1]);
		System.out.println(line[0].charAt(0));*/
		
		char temp= line[0].charAt(0);
		String tem1=Character.toString(temp);
		
		int count=Integer.parseInt(line[1]);
		Text word =new Text (tem1); 
		
		IntWritable val=new IntWritable(count); 
		context.write(word, val); 
	}
}