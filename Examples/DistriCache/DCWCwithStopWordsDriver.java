package DistriCache;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration; 
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.StringUtils;

/*
 	we are going to give two text files as input for word counting and one file as pattern input (symbols to exclude from wordcounting)
 	so third file is given to all mappers that are going to run two input files. so that . , / such symbols are not counted.
 	add MRv2 jar files for this example and run on standalone/cluster hadoop mode. dont run in eclipse
 	http://hadoop.praveendeshmane.co.in/hadoop/hadoop-distributed-cache-example.jsp
 */

public class DCWCwithStopWordsDriver{
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		GenericOptionsParser optionParser = new GenericOptionsParser(conf, args);
		String[] remainingArgs = optionParser.getRemainingArgs();
		if (remainingArgs.length <2) {
			System.err.println("Usage: wordcount <in> <out> [-skip skipPatternFile]");
			System.exit(2);
		}
		Job job = Job.getInstance(conf, "word count");
		job.setJarByClass(DistriCache.DCWCwithStopWordsDriver.class);
		job.setMapperClass(DistriCache.DCWCwithStopWordsMapper.class); 
		job.setReducerClass(DistriCache.DCWCwithStopWordsReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		List<String> otherArgs = new ArrayList<String>();
		for (int i=0; i < remainingArgs.length; ++i) {
			if ("-skip".equals(remainingArgs[i])) {
				//for MRv1 with its libraries
				//DistributedCache.addCacheFile(new URI(/user/peter/cacheFile/testCache1), job.getConfiguration()); //in MRv1
				
				//for MRv2 with its libraries
				job.addCacheFile(new Path(remainingArgs[++i]).toUri()); // path can be given as file:/// or hdfs:// or http:// in commandline arguments while running
				job.getConfiguration().setBoolean("wordcount.skip.patterns", true);
			} else {
				otherArgs.add(remainingArgs[i]);
			}
		}
		
		//FileInputFormat.addInputPath(job, new Path(otherArgs.get(0)));
		FileInputFormat.setInputPaths(job, new Path(otherArgs.get(0)), new Path(otherArgs.get(1)));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs.get(2)));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}