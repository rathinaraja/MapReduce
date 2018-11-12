package practice;

import org.apache.hadoop.conf.Configuration; 
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat; 

public class NLineInputDriver{

	public static void main(String[] args) throws Exception {
		if (args.length != 2) {
			System.out.println("Two parameters are required for DriverNLineInputFormat- <input dir> <output dir>\n");
			System.exit(-1);
		}

		Configuration conf = new Configuration();	
		Job job = Job.getInstance(conf, "NLineInputFormat example"); 
		job.getConfiguration().set("fs.file.impl", "WinLocalFileSystem");
		//job.setJobName( "NLineInputFormat example");
		job.setJarByClass(practice.NLineInputDriver.class);		

		job.setMapperClass(practice.NLineMapper.class);
		job.setNumReduceTasks(0);
		
		job.setInputFormatClass(NLineInputFormat.class);
		NLineInputFormat.addInputPath(job, new Path(args[0]));
		job.getConfiguration().setInt("mapreduce.input.lineinputformat.linespermap", 100);
		LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
 