package ChainingMRjobs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*; 
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser; 

//method2: using job control for mapper1-->reducer1-->mapper2->redcuer2 
//you can break this Driver into two and run manually one by one by giving job 1 output directory to input of job2 as given below. but, this is the simplest mehtod
//hadoop jar chain.jar Job1 shakespeare output
//hadoop jar chain.jar Job2 output output2

//however, very similar way you can to mapper1-->mapper2->mapper3--> reducer1-->mapper4-->reducer2 ... in any fashion

public class JobChainDriverMethod2  {
	
	private static final String OUTPUT_PATH = "intermediate_output";

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();		
		if (otherArgs.length != 2) {
			System.err.println("Enter valid number of arguments <Inputdirectory>  <Outputlocation>");
			System.exit(-1);
		}
		Job job = Job.getInstance(conf); 
		job.getConfiguration().set("fs.file.impl", "WinLocalFileSystem");
		job.getConfiguration().set("mapreduce.output.textoutputformat.separator", "\t"); 
		
		Configuration conf1 = new Configuration();
	    Configuration conf2 = new Configuration();

	    Job job1 = new Job(conf1, "WC");
	    job1.getConfiguration().set("fs.file.impl", "WinLocalFileSystem");
	    job1.setJarByClass(ChainingMRjobs.JobChainDriverMethod2 .class);	    
	   
	    job1.setMapperClass(ChainingMRjobs.Mapper1.class);
	    job1.setReducerClass(ChainingMRjobs.Reducer1.class);
	    
	    job1.setOutputKeyClass(Text.class);
	    job1.setOutputValueClass(IntWritable.class);
	    
	    job1.setInputFormatClass(TextInputFormat.class);
		job1.setOutputFormatClass(TextOutputFormat.class);
	    ControlledJob cJob1 = new ControlledJob(conf1);
	    cJob1.setJob(job1);
	    FileInputFormat.addInputPath(job1, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job1, new Path(OUTPUT_PATH));

	    Job job2 = new Job(conf2, "WC of WC");
	    job2.getConfiguration().set("fs.file.impl", "WinLocalFileSystem");
	    job2.setJarByClass(ChainingMRjobs.JobChainDriverMethod2.class);	    
		
	    job2.setMapperClass(ChainingMRjobs.Mapper2.class);
	    job2.setReducerClass(ChainingMRjobs.Reducer2.class);
	    //job2.setSortComparatorClass(ReverseOrder.class); 
	    
	    job2.setOutputKeyClass(Text.class);
	    job2.setOutputValueClass(IntWritable.class);
	    
	    job2.setInputFormatClass(TextInputFormat.class);
		job2.setOutputFormatClass(TextOutputFormat.class);
		
		ControlledJob cJob2 = new ControlledJob(conf2);
	    cJob2.setJob(job2);
	    FileInputFormat.addInputPath(job2, new Path(OUTPUT_PATH+"/part*"));
	    FileOutputFormat.setOutputPath(job2, new Path(args[1]));

	    JobControl jobctrl = new JobControl("jobctrl");
	    jobctrl.addJob(cJob1);
	    jobctrl.addJob(cJob2);
	    cJob2.addDependingJob(cJob1);
	    jobctrl.run();	
	    
	    // delete jobctrl.run();
	  /*  Thread t = new Thread(jobctrl);
	    t.start();
	    String oldStatusJ1 = null;
	    String oldStatusJ2 = null;
	    while (!jobctrl.allFinished()) {
	      String status =cJob1.toString();
	      String status2 =cJob2.toString();
	      if (!status.equals(oldStatusJ1)) {
	        System.out.println(status);
	        oldStatusJ1 = status;
	      }
	      if (!status2.equals(oldStatusJ2)) {
	        System.out.println(status2);
	        oldStatusJ2 = status2;
	      }     
	     }
	    System.exit(0);*/
	}	
}
