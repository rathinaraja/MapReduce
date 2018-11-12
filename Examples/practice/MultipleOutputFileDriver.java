package practice;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat; 
import org.apache.hadoop.util.GenericOptionsParser;

/*usually output file of reducer has naming convention part-r-00000. now, we are going to control name ("part") and 
create multiple output files. we will put word that starts with same letter into that specific letter as a file 
we have pre-processed in mapper and have assigned context to multiple outputs in reducer*/

public class MultipleOutputFileDriver{
	public static void main(String[] args) throws Exception { 
		Configuration conf = new Configuration();
		//String[] files=new GenericOptionsParser(conf,arg0).getRemainingArgs();
		if(args.length <2) {
			System.err.println("Usage:<inputpath> <outputpath>");
			System.exit(-1);
		}		
		Job job = Job.getInstance(conf, "word count");
		//or Job job=Job.getInstance(getConf());
		job.getConfiguration().set("fs.file.impl", "WinLocalFileSystem");
		job.setJarByClass(practice.MultipleOutputFileDriver.class);  		
		
		job.setMapperClass(practice.MultipleOutputFileMapper.class);
		job.setReducerClass(practice.MultipleOutputFileReducer.class); 
		//job.setNumReduceTasks(0);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class); 
		
		/*FileInputFormat.setInputPaths(job, new Path("D:/Eclipse Project/SamplePrograms/input.txt"));
		FileOutputFormat.setOutputPath(job, new Path(files[1]));*/		
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		for (int i = 0; i < otherArgs.length - 1; ++i) {
		      FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
		}
		FileOutputFormat.setOutputPath(job,new Path(otherArgs[otherArgs.length - 1]));
		
		System.exit( job.waitForCompletion(true) ? 0 : 1);
	}	
}

 
