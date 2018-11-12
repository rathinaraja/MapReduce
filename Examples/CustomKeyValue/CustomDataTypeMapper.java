package CustomKeyValue;

import java.io.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

public class CustomDataTypeMapper extends Mapper<LongWritable,Text,CustomKey,IntWritable>{
	private static final IntWritable one=new IntWritable(1);
	private CustomKey cKey=new CustomKey();
	
	private IntWritable reqNo=new IntWritable();
	private Text URL=new Text();
	private Text reqDate=new Text();
	private Text timestamp=new Text();
	private Text IP=new Text();
	
	public void map(LongWritable key, Text value, Context context)throws IOException, InterruptedException{
		String str[]=value.toString().split("\t");
		reqNo.set(Integer.parseInt(str[0]));
		URL.set(str[1]);
		reqDate.set(str[2]);
		timestamp.set(str[3]);
		IP.set(str[4]);
		//System.out.println(URL +"\t"+ reqDate+ "\t"+ timestamp+ "\t"+IP+ "\t"+ reqNo);
		//pass values to setter methods 
		/*cKey.setRegNo(regNo);
		cKey.setURL(URL);
		cKey.setRegDate(regDate);
		cKey.setTime(time);
		cKey.setIP(IP); */
		
		cKey.set(URL, reqDate, timestamp,IP, reqNo);
		// when you send CKey only IP is sent. because from getter only IP is taken rest all are commented
		context.write(cKey, one);
	}
}
