package practice;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;

// in eclipse by default only one reducer is launched.  if you try to run more reducers exception will be thrown. so run it in Hadoop cluster
//What if you create more number of partitions than the number of reducers? 
//What if you create less number of partitions than the number of reducers?

public class WCPartitioner extends Partitioner<Text,IntWritable>{
	@Override
	public int getPartition(Text key, IntWritable value, int numred) {
		// TODO Auto-generated method stub
		String s=key.toString();
		if (s.length()<2)
			return 0;
		else if(s.length()>=2 && s.length()<=4)
			return 1;
		else
			return 2; 
	}
}



