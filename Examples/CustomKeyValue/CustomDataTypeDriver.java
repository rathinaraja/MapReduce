package CustomKeyValue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser; 

//objective is to find occurance of IP addresses from a log data set. it is not required to build custom data type. 
// but we create custom data type that include all fields of logs.
// map output displays only hashcode, but reducer output displays IP occurances. why is like that? try to get map output readable.. refer TitanicCustomKey
//you can create custom data type for key and value. if you create for key you can use the same data type for value also. but, datatype created for value cant be used for key as it needs comparing functions

//http://www.snnmo.com/blog/articles/big-data/creating-custom-hadoop-writable-data-type.shtml
//http://hadooptutorial.info/creating-custom-hadoop-writable-data-type/
//http://www.hadoopmaterial.com/2013/10/custom-hadoop-writable-data-type.html (very nice)

public class CustomDataTypeDriver {

	public static void main(String args[])throws Exception{
		// TODO Auto-generated method stub
		Configuration conf=new Configuration();
		String[] otherArgs=new GenericOptionsParser(conf,args).getRemainingArgs(); 
		if(args.length<2){
			System.err.print("input more than two arguments");
			System.exit(-1);
		}		
		Job job = Job.getInstance(conf, "custom data type"); 
		//Job job=new Job(conf,"custom data type");
		job.setJarByClass(CustomKeyValue.CustomDataTypeDriver.class);
		job.getConfiguration().set("fs.file.impl", "WinLocalFileSystem");		
		
		job.setMapperClass(CustomKeyValue.CustomDataTypeMapper.class);
		job.setReducerClass(CustomKeyValue.CustomDataTypeReducer.class); 
		//job.setCombinerClass(practice.WCMRv2Reducer.class);
		//job.setNumReduceTasks(0); 
		
		job.setMapOutputKeyClass(CustomKey.class);
		job.setMapOutputValueClass(IntWritable.class);		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class); 
				
		for(int i=0;i<otherArgs.length-1;i++){
			FileInputFormat.addInputPath(job,new Path(otherArgs[i]));
		}
		FileOutputFormat.setOutputPath(job,new Path(otherArgs[otherArgs.length - 1]));
		
		System.exit(job.waitForCompletion(true) ? 0 : 1); 
	}
}
