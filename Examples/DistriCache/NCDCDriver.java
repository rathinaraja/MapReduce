package DistriCache;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat; 
/*
  in NcdcMaxTempReducer it shows NullPointer Exception. understand the program and debug it. 
  input files are WeatherRecord.txt and station.txt
  add MRv2 jars. dont add MRv1 it will lead to exception.
  run in hadoop services. it doesnot run in Eclipse
 */

public class NCDCDriver{
		
	public static void main(String[] arg) throws Exception {
		if(arg.length !=2) {
			System.err.println("Usage:<inputpath> <outputpath>");
			System.exit(-1);
		}	

		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Distributed Cache");
		job.setJarByClass(DistriCache.NCDCDriver.class);			
			
		job.setMapperClass(DistriCache.NcdcMaxTempMapper.class);
		//job.setCombinerClass(DistriCache.NcdcMaxTempCombiner.class);
		job.setReducerClass(DistriCache.NcdcMaxTempReducer.class);
			
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		FileInputFormat.addInputPath(job, new Path(arg[0]));
		FileOutputFormat.setOutputPath(job, new Path(arg[1])); 
		
		//for MRv1 with its libraries
		//DistributedCache.addCacheFile(new URI("hdfs://node1:10001/input/station.txt"), job.getConfiguration()); 
		
		//for MRv2 with its libraries
		job.addCacheFile(new Path("hdfs://node1:10001/input/station.txt").toUri());  
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
