package CustomKeyValue;

import java.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;

public class CustomDataTypeReducer extends Reducer<CustomKey,IntWritable,Text,IntWritable> {
	
	private IntWritable result=new IntWritable();
	private Text IP=new Text();
	
	public void reduce(CustomKey key,Iterable<IntWritable> values, Context context)throws IOException, InterruptedException{
		int sum=0;
		IP=key.getIP();
		
		for(IntWritable val:values){
			sum++;
		}
		result.set(sum);
		context.write(IP,result);
	}
}
