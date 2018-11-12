package practice;

import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import practice.CounterDriver.COUNT; 
 
public class CounterMapper extends Mapper<LongWritable, Text, LongWritable, Text> {
	public void map(LongWritable key, Text value,Context context)throws IOException, InterruptedException {
		String[] fields = value.toString().split("\t");  
		//System.out.println(fields[2]);
		if (fields[2].equals("2014-03-10")) { 
			context.getCounter(COUNT.Date).increment(1); 
		}
		if(fields[4].equals("82.39.198.127")) {
			context.getCounter(COUNT.IP).increment(1); 
		}  
		context.write(new LongWritable(), new Text(value));
	}
}
