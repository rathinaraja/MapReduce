package ChainingMRjobs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat; 

// method1:cascading jobs  using job control for mapper1-->reducer1-->mapper2->redcuer2 
// you can break this Driver into two and run manually one by one by giving job 1 output directory to input of job2 as given below. but, this is the simplest mehtod
// hadoop jar chain.jar Job1 shakespeare output
// hadoop jar chain.jar Job2 output output2

//however, very similar way you can to mapper1-->mapper2->mapper3--> reducer1-->mapper4-->reducer2 ... in any fashion

public class JobChainDriverMethod1   {
	
	private static final String OUTPUT_PATH = "intermediate_output";

	public static void main(String[] args) throws Exception {
		if (args.length != 2) {
			System.err.println("Enter valid number of arguments <Inputdirectory>  <Outputlocation>");
			System.exit(-1);
		}
		
		// MR first job configuration for wordcount
		Configuration conf = new Configuration();
		Job job1 = new Job(conf, "Job1");
		job1.getConfiguration().set("fs.file.impl", "WinLocalFileSystem");
		job1.getConfiguration().set("mapreduce.output.textoutputformat.separator", "\t"); 

		job1.setInputFormatClass(TextInputFormat.class);
		job1.setOutputFormatClass(TextOutputFormat.class);
		
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(IntWritable.class);		
		
		job1.setMapperClass(ChainingMRjobs.Mapper1.class); 
		job1.setReducerClass(ChainingMRjobs.Reducer1.class); 
		
		TextInputFormat.addInputPath(job1, new Path(args[0]));
		TextOutputFormat.setOutputPath(job1, new Path(OUTPUT_PATH));
		
		job1.waitForCompletion(true); 
		
		// MR second job configuration for counting words that start with similar character from wordcont reuslt
  		Job job2 = new Job(conf, "Job 2");
		job2.setJarByClass(ChainingMRjobs.JobChainDriverMethod1.class);

		job2.setInputFormatClass(TextInputFormat.class);
	  	job2.setOutputFormatClass(TextOutputFormat.class);
	  	
	  	job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(IntWritable.class);		
	  	
		job2.setMapperClass(ChainingMRjobs.Mapper2.class);
		job2.setReducerClass(ChainingMRjobs.Reducer2.class);

		TextInputFormat.addInputPath(job2, new Path(OUTPUT_PATH));
	  	TextOutputFormat.setOutputPath(job2, new Path(args[1]));

		System.exit(job2.waitForCompletion(true) ? 0 : 1);
	}
}