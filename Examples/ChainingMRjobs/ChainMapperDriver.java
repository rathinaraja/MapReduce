package ChainingMRjobs;

import java.io.IOException;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper; 
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat; 

//method 3: Using ChainMapper and ChainReducer 
//include MRv2 jars and run and check in the cluster
//similarly you can do for chaindriver
public class ChainMapperDriver {
    
	 public static void main(String args[])throws Exception{
        
		Configuration conf=new Configuration();
        //bypassing the GenericOptionsParser part and directly running into job declaration part
        Job job=Job.getInstance(conf);
        job.getConfiguration().set("fs.file.impl", "WinLocalFileSystem");	
        
        /**************CHAIN MAPPER AREA STARTS********************************/
        Configuration conf1=new Configuration(false);
        //below we add the 1st mapper class under ChainMapper Class
        ChainMapper.addMapper(job, Mapper1.class, LongWritable.class, Text.class, Text.class, IntWritable.class, conf1);
        //configuration for second mapper
        Configuration conf2=new Configuration(false);
        //below we add the 2nd mapper that is the lower case mapper to the Chain Mapper class
        ChainMapper.addMapper(job, Mapper3.class, Text.class, IntWritable.class, Text.class, IntWritable.class,conf2);
        
        //you can chain of reducers too in this row
        /**************CHAIN MAPPER AREA FINISHES********************************/

        //now proceeding with the normal delivery
        job.setJarByClass(ChainingMRjobs.ChainMapperDriver.class);
        job.getConfiguration().set("mapreduce.output.textoutputformat.separator", "\t"); 
        job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        Path p=new Path(args[1]);

        //set the input and output URI
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, p);
        
        p.getFileSystem(conf).delete(p, true);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
