package Titanic;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable; 
import org.apache.hadoop.io.Text; 
import org.apache.hadoop.mapreduce.InputSplit; 
import org.apache.hadoop.mapreduce.RecordReader; 
import org.apache.hadoop.mapreduce.TaskAttemptContext; 
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader; 

public class TitanicRR extends RecordReader<TitanicCustomKey,IntWritable> { 
	
	private TitanicCustomKey key; 
	private IntWritable value; 
	private LineRecordReader reader = new LineRecordReader(); 
			
	@Override 
	public TitanicCustomKey getCurrentKey() throws IOException, InterruptedException {
		// TODO Auto-generated method stub 
		return key; 
	} 
	
	@Override 
	public IntWritable getCurrentValue() throws IOException, InterruptedException { 
		// TODO Auto-generated method stub 
		return value; 
	} 
	
	@Override 
	public float getProgress() throws IOException, InterruptedException { 
		// TODO Auto-generated method stub 
		return reader.getProgress(); 
	} 
	
	@Override 
	public void initialize(InputSplit is, TaskAttemptContext tac)throws IOException, InterruptedException { 
		reader.initialize(is, tac); 
	}
	
	@Override 
	//every key:value from dataset is passed to mapper
	
	public boolean nextKeyValue() throws IOException, InterruptedException { 
		// TODO Auto-generated method stub 
		boolean gotNextKeyValue = reader.nextKeyValue(); 
		if(gotNextKeyValue){ 
			if(key==null){ 
				key = new TitanicCustomKey(); 
			} 
			if(value == null){ 
				value = new IntWritable(); 
			} 
			Text line = reader.getCurrentValue(); 
			String[] tokens = line.toString().split(","); 
			//As discussed earlier, we need 2nd and 5th columns passed to our custom key.
			//The value is set as ‘1‘ since we need to count the number of people. so need not assign value 1 in mapper
			key.setX(new String(tokens[1])); 			
			key.setY(new String(tokens[4])); 
			value.set(new Integer(1)); 
		} 
		else { 
			key = null; 
			value = null; 
		}
		return gotNextKeyValue; 
	} 
	@Override 
	public void close() throws IOException { 
		// TODO Auto-generated method stub 
		reader.close(); 
	} 
}
