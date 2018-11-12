package practice; 
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat; 

public class WholeFileISDriver  {
	public static void main(String[] args) throws Exception {
		if (args.length != 2) {
			System.out.println("Two parameters are required for DriverNLineInputFormat- <input dir> <output dir>\n");
			System.exit(-1);
		}

		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "whole file IS");
		job.getConfiguration().set("fs.file.impl", "WinLocalFileSystem"); 
		job.setJarByClass(practice.WholeFileISDriver.class);			
		
		job.setMapperClass(practice.WholeFileMapper.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		job.setNumReduceTasks(0);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		job.setInputFormatClass(practice.WholeFileInputFormat.class);
		//if you dont use combinetext input format, then number of mappers is equal to number of input files
		// job.setInputFormatClass(CombineTextInputFormat.class);
				
		FileOutputFormat.setOutputPath(job,new Path(args[1]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
 
