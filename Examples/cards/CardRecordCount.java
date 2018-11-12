package cards;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration; 
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.LongSumReducer; 

public class CardRecordCount{
	
	private static class RecordMapper extends Mapper <LongWritable,Text,Text,IntWritable>{
		public void map(LongWritable lineoffset, Text record, Context output)throws IOException, InterruptedException{
			output.write(new Text("count"), new IntWritable(1));
		}
	}
	
	private static class RecordReducer extends Reducer<Text,IntWritable,Text,IntWritable>{
		public void reduce(Text key,Iterable<IntWritable> values, Context output)throws IOException, InterruptedException{
			int sum = 0;
		      for (IntWritable val : values) {
		        sum += val.get();
		      }
		     output.write(new Text("total counte \t"), new IntWritable(sum));
		}
	}
	public static void main(String args[]) throws Exception{
		// TODO Auto-generated method stub
		if(args.length <2) {
			System.err.println("Usage:<inputpath> <outputpath>");
			System.exit(-1);
		}
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		job.getConfiguration().set("fs.file.impl", "WinLocalFileSystem");		
		
		job.setMapperClass(RecordMapper.class);
		job.setReducerClass(RecordReducer.class);  // user defined class for counting
		//job.setReducerClass(LongSumReducer.class);  //inbuilt class for counting.
		//job.setNumReduceTasks(0);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);		
	}

}