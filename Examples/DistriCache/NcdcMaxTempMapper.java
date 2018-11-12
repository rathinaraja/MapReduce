package DistriCache;
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class NcdcMaxTempMapper extends Mapper<LongWritable,Text,Text,IntWritable>{

	protected void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException{
		NcdcRecordParcer parser = new NcdcRecordParcer();
		parser.parse(value);
		context.write(new Text(parser.getStationId()), new IntWritable(parser.getAirTemparature()));
	}
}
