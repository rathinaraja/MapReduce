package cards;


import java.io.IOException;

import org.apache.hadoop.conf.Configuration; 
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;  
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat; 
import org.apache.hadoop.mapreduce.lib.reduce.LongSumReducer; 

public class CardCountBySuite {

	public static class Map extends Mapper<LongWritable,Text,Text,LongWritable>{
		public void map(LongWritable key, Text value, Context output) throws IOException, InterruptedException{
			output.write(new Text(value.toString().split("\\|")[1]), new LongWritable(1));
		}
	}
	
	public static void main(String args[]) throws Exception{
		if(args.length <2) {
			System.err.println("Usage:<inputpath> <outputpath>");
			System.exit(-1);
		}
		Configuration conf=new Configuration();
		Job job=Job.getInstance(conf); 
		job.getConfiguration().set("fs.file.impl", "WinLocalFileSystem");
		
		job.setMapperClass(Map.class);	
		job.setReducerClass(LongSumReducer.class);
		//job.setPartitionerClass(HashPartitioner.class);
		//job.setNumReduceTasks(0);
		
		//job.setMapOutputKeyClass(Text.class);
		//job.setMapOutputValueClass(LongWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job,new Path(args[1]));
		
		System.exit(job.waitForCompletion(true) ? 0 : 1); 
	}
}
