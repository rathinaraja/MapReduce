package MRv1;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
//import org.apache.hadoop.mapred.TextInputFormat;
//import org.apache.hadoop.mapred.TextOutputFormat;

import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.Reducer;

import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner; 
	
public class WCMRv1AllinOne extends Configured implements Tool{
	
	public static class UDFmapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();
		public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
			String line = value.toString();
			StringTokenizer tokenizer = new StringTokenizer(line);
			while (tokenizer.hasMoreTokens()) {
				word.set(tokenizer.nextToken());
				output.collect(word, one);
			}
		}
	}

	public static class UDFreducer extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {
		public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
			int sum = 0;
			while (values.hasNext()) {
				sum += values.next().get();
			}
			output.collect(key, new IntWritable(sum));
		}
	}
	
	public int run(String[] args) throws Exception {
		if(args.length <2) {
			System.err.println("Usage:<inputpath> <outputpath>");
			System.exit(-1);
		}
		
		JobConf job = new JobConf(MRv1.WCMRv1AllinOne.class); 
		job.set("fs.file.impl", "WinLocalFileSystem");
		job.setJarByClass(MRv1.WCMRv1AllinOne.class);  
				
		job.setMapperClass(UDFmapper.class); 
		job.setReducerClass(UDFreducer.class);
		//job.setNumReduceTasks(0);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);  
		
		//job.setInputFormat(TextInputFormat.class);
		//job.setOutputFormat(TextOutputFormat.class);
		
		//FileInputFormat.setInputPaths(job, new Path("D:/Eclipse Project/SamplePrograms/input.txt"));
		//FileOutputFormat.setOutputPath(job, new Path("D:/Eclipse Project/SamplePrograms/output"));
		FileInputFormat.addInputPath(job, new Path(args[0]));  	
		FileOutputFormat.setOutputPath(job, new Path(args[1]));  	
				
		JobClient.runJob(job);
		return 0;
	}
	public static void main(String[] args) throws Exception { 
		int exitCode = ToolRunner.run(new MRv1.WCMRv1AllinOne(), args);
		System.exit(exitCode);		
  	}
} 




  	