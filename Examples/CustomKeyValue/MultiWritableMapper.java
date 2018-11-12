package CustomKeyValue;

import java.io.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

public class MultiWritableMapper extends Mapper<Object, Text, Text, MultiWritableValue>{ 
	private Text IP = new Text();
	private Text URL = new Text();
	private IntWritable count = new IntWritable(1);
	
	public void map(Object key, Text value,Context context)throws IOException, InterruptedException{ 
		
		//56682187	/vfnlyvnx.html	2014-03-10	12:22:10	191.170.95.175
		String[] str=value.toString().split("\t");
		//System.out.println(str);
		URL.set(str[1]);
		IP.set(str[4]);
		 
		context.write(URL,	new MultiWritableValue(IP));
		context.write(URL,	new MultiWritableValue(count));		
		// output of mapper is key:/vfnlyvnx.html  value: 191.170.95.175 and 1
	} 
}
