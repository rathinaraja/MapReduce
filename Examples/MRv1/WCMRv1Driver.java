package MRv1;
import org.apache.hadoop.conf.Configured;  
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*; 
import org.apache.hadoop.util.*;

public class WCMRv1Driver extends Configured implements Tool{
	public int run(String[] args) throws Exception {		
		if(args.length <2) {
			System.err.println("Usage:<inputpath> <outputpath>");
			System.exit(-1);
		}	
		JobConf job = new JobConf(MRv1.WCMRv1Driver.class); 
		job.set("fs.file.impl", "WinLocalFileSystem");
		job.setJarByClass(MRv1.WCMRv1Driver.class);		
				
		job.setMapperClass(MRv1.WCMRv1Mapper.class);
		job.setReducerClass(MRv1.WCMRv1Reducer.class);
		//job.setSortComparatorClass(ReverseOrder.class);
		//job.setNumReduceTasks(0);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);  
		
		//job.setInputFormat(TextInputFormat.class);
		//job.setOutputFormat(TextOutputFormat.class);
		
		//FileInputFormat.setInputPaths(job, new Path("D:/Eclipse Project/SamplePrograms/input.txt"));
		//FileOutputFormat.setOutputPath(job, new Path("D:/Eclipse Project/SamplePrograms/output"));
		FileInputFormat.addInputPath(job, new Path(args[0]));  	
		FileOutputFormat.setOutputPath(job, new Path(args[1]));  
					
		JobClient.runJob(job);
		return 0;			
	}
	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new WCMRv1Driver(), args);
		System.exit(exitCode);
	}
}

 