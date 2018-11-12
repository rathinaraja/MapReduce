package Titanic;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat; 

/* 	https://acadgild.com/blog/custom-input-format-hadoop/
 	problem statement: Find out the number of people who died and survived, along with their genders.
 	There are 12 columns in Titanic dataset 
	PassengerId	Survived(survived=0&died=1)	Pclass	Name  Sex Age SibSp	Parch	Ticket	Fare Cabin 	Embarked

	step1: implement a custom key which is a combination of two columns 2nd and 5th column
	step2: create custom InputFormat
	step3: implement custom Record Reader
	step4: mapper using custom key and value
	step5: reducer using custom key and value
	step6: driver method
	
*/
public class TitanicCustomInputFormatDriver  {
	
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		Configuration conf=new Configuration();
		if(args.length<2){
			System.err.print("input more than two arguments");
			System.exit(-1);
		}	
		Job job = Job.getInstance(conf, "custom input format"); 
		job.getConfiguration().set("fs.file.impl", "WinLocalFileSystem");
		job.setJarByClass(TitanicCustomInputFormatDriver.class);
		
		job.setMapperClass(TitanicMapper.class);		 
		job.setReducerClass(TitanicReducer.class);
					 
		job.setMapOutputKeyClass(TitanicCustomKey.class);		 
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(TitanicCustomKey.class);	
		job.setOutputValueClass(IntWritable.class);			
		 
		/*Path out=new Path(args[1]);		 
		out.getFileSystem(conf).delete(out);*/		 
		FileInputFormat.addInputPath(job,new Path( args[0]));	
		job.setInputFormatClass(TitanicInputFormat.class);		
		FileOutputFormat.setOutputPath(job, new Path(args[1]));			 
		job.setOutputFormatClass(TextOutputFormat.class);	
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);		
	}
}

