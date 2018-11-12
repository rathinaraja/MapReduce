package Titanic;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable; 
import org.apache.hadoop.mapreduce.InputSplit; 
import org.apache.hadoop.mapreduce.RecordReader; 
import org.apache.hadoop.mapreduce.TaskAttemptContext; 
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

/*
 implement custom input format by extending the default FileInputFormat, which accepts the parameters key 
 and value as our custom _key and the value as IntWritable. Now, these values are passed to the Record reader, 
 which does the actual formatting of the inputs.   *
 */
public class TitanicInputFormat  extends FileInputFormat<TitanicCustomKey,IntWritable> { 
	@Override 
	public RecordReader<TitanicCustomKey,IntWritable> createRecordReader(InputSplit arg0,TaskAttemptContext arg1) throws IOException, InterruptedException { 
		return new TitanicRR(); 
	} 
}