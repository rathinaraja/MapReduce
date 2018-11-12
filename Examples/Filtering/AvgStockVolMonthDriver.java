package Filtering;

import java.io.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;  // it is in hadoop-mapreduce-client-core-2.7.0
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat; 
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat; 
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import NYSE.CustomKeyValue.*;
import NYSE.AvgStockVolPerMonth.*; 

public class AvgStockVolMonthDriver extends Configured implements Tool{
	 public int run(String args[]) throws IOException, InterruptedException, ClassNotFoundException {
		 if (args.length < 2) {
	    	System.err.println("Usage: wordcount <in> [<in>...] <out>");
	    	System.exit(2);
	    }    
		Configuration conf=getConf(); 
		Job job=Job.getInstance(conf,"NYSE average stock volume per month");
		//job.getConfiguration().set("fs.file.impl", "WinLocalFileSystem");
		//job.setJarByClass(getClass());
		job.setJarByClass(NYSE.AvgStockVolPerMonth.AvgStockVolMonthDriver.class);
		
		job.setMapperClass(AvgStockMapperWithSetUp.class);
		//average of average is not right. so we define new combiner class rather reusing reducer task
		job.setCombinerClass(AvgStockVolMonthCombiner.class);
		//now check combiner output		
		job.setReducerClass(AvgStockVolMonthReducer.class);
		job.setNumReduceTasks(4);  // to check mapper output
		
		job.setMapOutputKeyClass(TextPair.class);
		job.setMapOutputValueClass(LongPair.class);  		
		job.setOutputKeyClass(TextPair.class);
		job.setOutputValueClass(LongPair.class); 
		
		job.setInputFormatClass(CombineTextInputFormat.class);  // just enable this if you have more number of small files to pack as one IS.  It is in hadoop-mapreduce-client-core-2.7.0
		CombineTextInputFormat.setMaxInputSplitSize(job, 64000000);		// many files are put into 64 MB
		//job.setInputFormatClass(TextInputFormat.class); 
		//job.setOutputFormatClass(TextOutputFormat.class);
				
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		for (int i = 0; i < otherArgs.length - 1; ++i) {
		      FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
		}
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[otherArgs.length - 1])); 
	/*	FileInputFormat.addInputPath(job, new Path("D:/Eclipse Project/Practice/dataset/nyse_data/*.csv"));
	 	FileOutputFormat.setOutputPath(job, new Path("D:/Eclipse Project/Practice/output"));*/
						
		return job.waitForCompletion(true) ? 0 : 1;
	} 
	public static void main(String args[])throws Exception{ 
		int exitCode=ToolRunner.run(new AvgStockVolMonthDriver(), args);
		 System.exit(exitCode);	    
	}
}
