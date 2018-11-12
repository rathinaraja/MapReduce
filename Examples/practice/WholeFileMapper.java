package practice;
import java.io.IOException;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper; 

public class WholeFileMapper  extends Mapper<Object, BytesWritable, NullWritable, Text> {

	@Override
	public void map(Object key, BytesWritable value, Context context) throws IOException, InterruptedException {
		byte[] bytes=value.getBytes();
    	String out = new String(bytes);//.toString(); //value.toString();
		context.write(NullWritable.get(), new Text(out));
	}

}
 