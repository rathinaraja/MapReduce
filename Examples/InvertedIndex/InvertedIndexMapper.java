package InvertedIndex;
import java.io.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.StringUtils;

public class InvertedIndexMapper extends Mapper<LongWritable,Text,Text,Text>{
	private Text docID;
	private Text word=new Text();
	
	protected void setup(Context context){
		String filename=((FileSplit)context.getInputSplit()).getPath().getName();
		docID=new Text(filename);
	}
	protected void map(LongWritable key, Text value,Context context) throws IOException, InterruptedException{
		String[] temp=value.toString().split(" ");		
		for(String token:temp){
			word.set(token);
			context.write(word,docID);
		}
	}
}
