package MRv2;

import java.io.IOException;
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
import org.apache.hadoop.util.GenericOptionsParser;

public class MRv2AllinOne{
	public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable>{    
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();
      
		public void map(Object key, Text value, Context context ) throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString());
			while (itr.hasMoreTokens()) {
				word.set(itr.nextToken());
				context.write(word, one);
			}
		}
	}
  
	public static class IntSumReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values, Context context ) throws IOException, InterruptedException {
    	int sum = 0;
    	for (IntWritable val : values) {
    		sum += val.get();
    	}
    	result.set(sum);
    	context.write(key, result);
    	}
	}

	public static void main(String[] args) throws Exception {		
		if (args.length < 2) {
			System.err.println("Usage: wordcount <in> [<in>...] <out>");
			System.exit(2);
		}
		Configuration conf = new Configuration();	
		Job job = Job.getInstance(conf, "word count");
		job.getConfiguration().set("fs.file.impl", "WinLocalFileSystem");
		job.setJarByClass(MRv2.MRv2AllinOne.class);		
		
		//job.setInputFormatClass(TextInputFormat.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(IntSumReducer.class);
		job.setNumReduceTasks(0);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		for (int i = 0; i < otherArgs.length - 1; ++i) {
			FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
		}
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[otherArgs.length - 1]));
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}



