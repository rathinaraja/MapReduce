package InvertedIndex;
import java.io.*;
import java.util.*;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

public class InvertedIndexReducer extends Reducer<Text,Text,Text,Text> {
	private Text docIDs=new Text();
	protected void reduce(Text key,Iterable<Text> values,Context context)throws IOException, InterruptedException{
		HashSet<String> uniqueDocIDs=new HashSet<String>();
		for(Text docID:values){
			uniqueDocIDs.add(docID.toString());
		}
		docIDs.set(new Text(StringUtils.join(uniqueDocIDs,",")));
		context.write(key, docIDs);
	}
}
