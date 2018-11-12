package Titanic;

import java.io.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

//The Reducer will count all the values for each unique Reducer.
//The Keys have been sorted by the gender column. 
public class TitanicReducer extends Reducer<TitanicCustomKey, IntWritable, TitanicCustomKey, IntWritable> {	 
	public void reduce(TitanicCustomKey key, Iterable<IntWritable> values, Context context)throws IOException, InterruptedException { 
		int sum = 0; 
		for (IntWritable val : values) { 
			sum += val.get(); 
		} 
		context.write(key, new IntWritable(sum)); 
	} 
}