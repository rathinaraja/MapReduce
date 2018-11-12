package practice;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration; 
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat; 


public class SmallFilesToSequenceFileConverter {
	static class SequenceFileMapper extends Mapper<NullWritable, BytesWritable, Text, BytesWritable> {
		private Text filenameKey;
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			InputSplit split = context.getInputSplit();
			Path path = ((FileSplit) split).getPath();
			filenameKey = new Text(path.toString());
		}
		@Override
		protected void map(NullWritable key, BytesWritable value, Context context)throws IOException, InterruptedException {
			context.write(filenameKey, value);
		}
	}
	
	public static void main(String[] args) throws Exception {
		if(args.length !=2) {
			System.err.println("Usage:<inputpath> <outputpath>");
			System.exit(-1);
		}	

		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "word count");
		//or Job job=Job.getInstance(getConf());
		job.getConfiguration().set("fs.file.impl", "WinLocalFileSystem"); 		
		
		job.setMapperClass(SequenceFileMapper.class);
		job.setNumReduceTasks(0);
		
		job.setInputFormatClass(WholeFileInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(BytesWritable.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job,new Path(args[1]));
		
		job.waitForCompletion(true);
	}
}
