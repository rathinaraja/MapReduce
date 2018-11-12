package CustomKeyValue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser; 

//data set is log data set
//multiple data type in mapper output

public class MultiWritableDriver {

	public static void main(String args[])throws Exception{
		// TODO Auto-generated method stub		
		Configuration conf=new Configuration();
		if(args.length<1){
			System.err.print("input more than two arguments");
			System.exit(-1);
		}	
		Job job= Job.getInstance(conf, "multiple data type in mapper output");
		job.setJarByClass(CustomKeyValue.MultiWritableDriver.class);
		job.getConfiguration().set("fs.file.impl", "WinLocalFileSystem");		
		
		job.setMapperClass(CustomKeyValue.MultiWritableMapper.class);
		job.setReducerClass(CustomKeyValue.MultiWritableReducer.class); 
		//job.setCombinerClass(practice.WCMRv2Reducer.class);
		//job.setNumReduceTasks(0);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(MultiWritableValue.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);		
		
		String[] otherArgs=new GenericOptionsParser(conf,args).getRemainingArgs(); 
		for(int i=0;i<otherArgs.length-1;i++){
			FileInputFormat.addInputPath(job,new Path(otherArgs[i]));
		}
		FileOutputFormat.setOutputPath(job,new Path(otherArgs[otherArgs.length - 1]));
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);		
	}
}

