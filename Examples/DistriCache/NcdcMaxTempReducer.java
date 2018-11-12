package DistriCache;
import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer; 

public class NcdcMaxTempReducer extends Reducer<Text,IntWritable,Text,IntWritable>{
	
	private NcdcStationMetadataParcer metadata;
	private Configuration conf;
	String fileName ;
	String stationName;
	
	protected void setup(Context context) throws IOException{
		try{
			URI[] patternsURIs = Job.getInstance(conf).getCacheFiles(); 
			for (URI patternsURI : patternsURIs) { 
    			Path patternsPath = new Path(patternsURI.getPath());
    			fileName = patternsPath.getName().toString().trim(); 
    			if (fileName.equals("station.txt")){
					metadata = new NcdcStationMetadataParcer();
					metadata.initialize(patternsPath);
					break;
				}
			}			
			System.out.println("File : "+ patternsURIs[0].toString());
		}
		catch(NullPointerException e){
			System.out.println("Exception : "+e);
		}		
		System.out.println("Cache : "+context.getConfiguration().get("mapred.cache.files"));
	}
	
	protected void reduce(Text key,Iterable<IntWritable> values,Context context) throws IOException, InterruptedException{	

		System.out.println("step1");
		stationName = metadata.getStationName(key.toString());

		System.out.println("step2");
		int maxValue = Integer.MIN_VALUE;
		for (IntWritable val:values){
			maxValue = Math.max(maxValue, val.get());
		}
		context.write(new Text(stationName),new IntWritable(maxValue));
	}
}
