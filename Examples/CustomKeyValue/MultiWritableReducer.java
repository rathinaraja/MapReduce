package CustomKeyValue;

import java.io.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

// output of mapper is key:/vfnlyvnx.html  value: 191.170.95.175 and 1

public class MultiWritableReducer extends Reducer<Text,MultiWritableValue,Text,IntWritable>{
	
	private Text result = new Text();
	public void reduce(Text key,Iterable<MultiWritableValue> values, Context context) throws IOException, InterruptedException{
		
		int sum = 0;
		StringBuilder output = new StringBuilder();
		
		for (MultiWritableValue multi : values) {
			Writable writable = multi.get();
			if (writable instanceof IntWritable){
				sum += ((IntWritable)writable).get();
			}
			/*else{
				requests.append(((Text)writable).toString());
				requests.append("\t");
			}*/
		}		
		//result.set(key + "\t"+sum);
		context.write(key, new IntWritable(sum));
	}
}
