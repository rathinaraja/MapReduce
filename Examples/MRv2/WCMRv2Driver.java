package MRv2;
import org.apache.hadoop.conf.Configuration; 
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser; 

public class WCMRv2Driver  { 
	public static void main(String[] args) throws Exception { 
		if(args.length <2) {
			System.err.println("Usage:<inputpath> <outputpath>");
			System.exit(-1);
		}	

		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "word count");
		//or Job job=Job.getInstance(getConf());
		job.getConfiguration().set("fs.file.impl", "WinLocalFileSystem");
		job.setJarByClass(MRv2.WCMRv2Driver.class); 		
			
		job.setMapperClass(MRv2.WCMRv2Mapper.class);
		job.setReducerClass(MRv2.WCMRv2Reducer.class); 
		//job.setNumReduceTasks(5);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);		
		//job.setSortComparatorClass(ReverseOrder.class);	
		
		//job.setInputFormatClass(TextInputFormat.class);
		//job.setOutputFormatClass(TextOutputFormat.class);
		
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		for (int i = 0; i < otherArgs.length - 1; ++i) {
		      FileInputFormat.addInputPath(job, new Path("hdfs://10.100.52.142:10001/input.txt"));
		}
		FileOutputFormat.setOutputPath(job,new Path("hdfs://10.100.52.142:10001/output"));
		
		//FileInputFormat.addInputPath(job, new Path(args[0]));
		//FileInputFormat.addInputPath(job, new Path("D:/Eclipse Project/Practice/input.txt"));
		//FileOutputFormat.setOutputPath(job,new Path(args[1]));
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}	
}

 