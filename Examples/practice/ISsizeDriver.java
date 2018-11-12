package practice;
import org.apache.hadoop.conf.Configuration; 
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.*;		  

public class  ISsizeDriver {
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
		job.setJarByClass(practice.ISsizeDriver.class);  		
			
		job.getConfiguration().set("mapreduce.input.fileinputformat.split.maxsize", "1000");   // in bytes
		job.getConfiguration().set("mapreduce.input.fileinputformat.split.minsize", "1000");   // in bytes
		
		/*changing block size after stored in HDFS is time consuming process. therefore, decide and set blocks size
		well in advance when you upload data onto HDFS. Therefore, IS is used at run time to customize the block size logically*/
		
		job.setMapperClass(practice.WCMRv2Mapper.class);
		job.setReducerClass(practice.WCMRv2Reducer.class); 
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class); 
		//job.setCombinerClass(practice.WCMRv2Reducer.class);
		job.setNumReduceTasks(4);
		
		/*FileInputFormat.setInputPaths(job, new Path(files[0]));
		FileOutputFormat.setOutputPath(job, new Path(files[1]));*/
				String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		for (int i = 0; i < otherArgs.length - 1; ++i) {
		      FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
		}
		FileOutputFormat.setOutputPath(job,new Path(otherArgs[otherArgs.length - 1]));
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);		 
	}
}

  
