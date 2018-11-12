package practice;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.*;  

public class CounterDriver {
	
	//creating counters for log.txt to check similar date, IP
	public static enum COUNT {
		  Date,IP;
	};
	
	public static void main(String[] args) throws Exception {
			if(args.length < 2) {
				System.err.println("input minimum two arguments");
				System.exit(-1);
			}
			Configuration conf=new Configuration(); 
			Job job=Job.getInstance(conf,"counter job"); 
			job.getConfiguration().set("fs.file.impl", "MRv2.WinLocalFileSystem");
			job.setJarByClass(practice.CounterDriver.class); 			
			
			job.setMapperClass(practice.CounterMapper.class); 
			job.setMapOutputKeyClass(LongWritable.class);
			job.setMapOutputValueClass(Text.class); 
			job.setNumReduceTasks(0);
			
			FileInputFormat.addInputPath(job, new Path(args[0])); 
			FileOutputFormat.setOutputPath(job, new Path(args[1]));
			
			System.exit(job.waitForCompletion(true) ? 0 : 1);
			
			Counters counters = job.getCounters();
			// to display user defined counters
			Counter c1 = counters.findCounter(COUNT.Date);
			System.out.println(c1.getDisplayName()+":"+c1.getValue());
			Counter c2 = counters.findCounter(COUNT.IP);
			System.out.println(c2.getDisplayName()+":"+c2.getValue());
			
			/*long date = job.getCounters().findCounter("Date", "debt").getValue();
			long mortage = job.getCounters().findCounter("Mortgage-Counter", "mortgage").getValue();
			long other = job.getCounters().findCounter("Other-Counter", "other").getValue();*/
			
			// to display inbuilt counters along with user defined
			for (CounterGroup group : counters) {
				  System.out.println("* Counter Group: " + group.getDisplayName() + " (" + group.getName() + ")");
				  System.out.println("  number of counters in this group: " + group.size());
				  for (Counter counter : group) {
				    System.out.println("  - " + counter.getDisplayName() + ": " + counter.getName() + ": "+counter.getValue());
				  }
			}		
	}			
}