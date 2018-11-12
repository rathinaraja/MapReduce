package practice;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;  

public class CombineFilesDriver {
	public static void main(String[] args) throws Exception { 
		Configuration conf = new Configuration(); 
		Job job = Job.getInstance(conf, "word count");
		//or Job job=Job.getInstance(getConf());
		job.getConfiguration().set("fs.file.impl", "WinLocalFileSystem");
		job.setJarByClass(practice.CombineFilesDriver.class);  		
		
		job.setMapperClass(practice.WCMRv2Mapper.class);	// check number of map output and it is not sorted
		//job.setReducerClass(practice.WCMRv2Reducer.class); 
		job.setNumReduceTasks(0);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class); 
		
		job.setInputFormatClass(CombineTextInputFormat.class);  // just enable this if you have more number of small files to pack as one IS.  It is in hadoop-mapreduce-client-core-2.7.0
		CombineTextInputFormat.setMaxInputSplitSize(job, 64000000);		// many files are put into 64 MB
		
		FileInputFormat.setInputPaths(job, new Path("D:/Eclipse Project/Practice/dataset/many files/*.txt"));
		FileOutputFormat.setOutputPath(job,new Path("D:/Eclipse Project/Practice/output"));
		
		System.exit( job.waitForCompletion(true) ? 0 : 1);
	}	
}

 