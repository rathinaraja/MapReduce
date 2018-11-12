package RunTimeArguments;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat; 
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class WCMRv2Driver extends Configured implements Tool{
	public int run(String args[]) throws IOException, InterruptedException, ClassNotFoundException {
		if(args.length <2) {
			System.err.println("Usage:<inputpath> <outputpath>");
			System.exit(-1);
		}	
		Configuration conf = getConf(); 
		//Configuration conf=new Configuration();
		//or Job job=Job.getInstance(getConf());
		Job job = Job.getInstance(conf, "word count");
		job.getConfiguration().set("fs.file.impl", "WinLocalFileSystem");
		//job.setJarByClass(practice.WCMRv2Driver.class);  	
		job.setJarByClass(getClass());
		
		job.setMapperClass(WCMRv2Mapper.class);
		job.setReducerClass(WCMRv2Reducer.class); 
		//job.setNumReduceTasks(0);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class); 
		
		//conf.set("LowerLimit", "2");
		//conf.set("UpperLimit", "5");		
		
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		for (int i = 0; i < otherArgs.length - 1; ++i) {
		      FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
		}
		//FileOutputFormat.setOutputPath(job, new Path(otherArgs[otherArgs.length - 1]));
		Path outpath = new Path(otherArgs[otherArgs.length - 1]); 
		outpath.getFileSystem(conf).delete(outpath); 
		FileOutputFormat.setOutputPath(job,outpath);		
		/*FileInputFormat.setInputPaths(job, new Path("D:/Eclipse Project/SamplePrograms/input.txt"));
		FileOutputFormat.setOutputPath(job, new Path(files[1]));*/		
		
		return job.waitForCompletion(true) ? 0 : 1;
	}	
	
	public static void main(String[] args) throws Exception { 
		int exitCode=ToolRunner.run(new WCMRv2Driver(), args);
		System.exit(exitCode);	
	}
}

 