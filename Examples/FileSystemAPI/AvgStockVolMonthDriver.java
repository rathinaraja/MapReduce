package FileSystemAPI;

import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import NYSE.AvgStockVolPerMonth.*;
import NYSE.CustomKeyValue.*;

// args[0] - first file path, args[1]- path to filter, paths[2]- output
// run this job in ubuntu eclipse or hadoop

public class AvgStockVolMonthDriver{
	public static void main(String args[])throws Exception{
		Configuration conf=new Configuration();
		Job job=Job.getInstance(conf,"NYSE average stock volume per month");
		job.getConfiguration().set("fs.file.impl", "WinLocalFileSystem");
		job.setJarByClass(FileSystemAPI.AvgStockVolMonthDriver.class);		
				
		job.setMapperClass(AvgStockVolMonthMapper.class);
		//job.setCombinerClass(AvgStockVolMonthCombiner.class);
		job.setReducerClass(AvgStockVolMonthReducer.class);
		//job.setNumReduceTasks(2);
		
		job.setMapOutputKeyClass(TextPair.class);
		job.setMapOutputValueClass(LongPair.class);  
		job.setOutputKeyClass(TextPair.class);
		job.setOutputValueClass(LongPair.class);
		
		FileSystem fs=FileSystem.get(URI.create(args[0]),conf);  // pass a directory
		//Path path=new Path(args[0]+"/*"); 					// you can filter
		//Path path=new Path(args[0]+"/*");   	
 		//Path path=new Path(args[0]+"/BySuite.java"); 
 		//Path path=new Path(args[0]+"/nyse_201?.*");
 		Path path=new Path(args[0]+"/nyse_201[1-2].*");
		FileStatus[] status=fs.globStatus(path);  				// to show list of files in a directory
		Path[] paths=FileUtil.stat2Paths(status);
		for(Path p:paths){
			System.out.println(p.toString()); 
			FileInputFormat.addInputPath(job,p);
		}
		job.setInputFormatClass(CombineTextInputFormat.class);
		//FileInputFormat.addInputPath(job, new Path("D:/14.HADOOP SOURCE CODE BIG DATASET/DATASET/data-master/nyse/nyse_data/"));
	 	//FileInputFormat.setInputPaths(job, new Path(args[0])); 
		FileOutputFormat.setOutputPath(job, new Path(args[1]));    
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
