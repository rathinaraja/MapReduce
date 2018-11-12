package Titanic;

import java.io.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

//The Mapper class will just emit the keys and values, as it is sent by the RecordReader.

public class TitanicMapper extends Mapper<TitanicCustomKey, Text, TitanicCustomKey, IntWritable> {	 
	private final static IntWritable one = new IntWritable(1); 
	public void map(TitanicCustomKey key, IntWritable value, Context context ) throws IOException, InterruptedException { 
		context.write(key, one); 
	} 
}