package MRUnitTest;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

//http://hadooptutorial.info/mrunit-example-for-wordcount-algorithm/
//https://sharebigdata.wordpress.com/2015/12/25/mr-unit-testing/
//http://appsintheopen.com/posts/40-unit-testing-map-reduce-programs-with-mrunit

//add MRv1 lib jars, add MRv2 lib jars, add MRv2 major common, hdfs, mapreduce, yarn jars
//add run with mrunit-1.0.0-hadoop2.jar and MRv2 jars

public class MRDriverUnitTest {
	MapReduceDriver<LongWritable, Text, Text, IntWritable, Text, IntWritable> mapReduceDriver;
	MapDriver<LongWritable, Text, Text, IntWritable> mapDriver;
	ReduceDriver<Text, IntWritable, Text, IntWritable> reduceDriver;
	
	@Before
	public void setUp() {		
		//MRUnitTest.MRv2AllinOne.UDFmapper mapper=new MRUnitTest.MRv2AllinOne.UDFmapper();
		//MRUnitTest.MRv2AllinOne.UDFreducer reducer=new MRUnitTest.MRv2AllinOne.UDFreducer();
		MRUnitTest.WCMRv2Mapper mapper=new MRUnitTest.WCMRv2Mapper();	
		MRUnitTest.WCMRv2Reducer reducer=new MRUnitTest.WCMRv2Reducer ();
		mapDriver = MapDriver.newMapDriver(mapper); 
	    reduceDriver = ReduceDriver.newReduceDriver(reducer);
	    mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper,reducer);
	}
	
	@Test
	public void testMapper() throws IOException {
		mapDriver.withInput(new LongWritable(), new Text("hi how are you you"));
		
		// here you can follow any order, that is are can come first then hi...
	    mapDriver.withOutput(new Text("hi"), new IntWritable(1));
	    mapDriver.withOutput(new Text("how"), new IntWritable(1));
	    mapDriver.withOutput(new Text("are"), new IntWritable(1));
	    mapDriver.withOutput(new Text("you"), new IntWritable(1));
	    mapDriver.withOutput(new Text("you"), new IntWritable(1));
	    mapDriver.runTest();
	}
	
	 @Test
	  public void testReducer() throws IOException {
		//you know input of reducer is sorted key with list of values. so give in sorted order 
		List<IntWritable> values = new ArrayList<IntWritable>();
		values.add(new IntWritable(1)); 
		reduceDriver.withInput(new Text("are"), values);
		    
	    List<IntWritable> values1 = new ArrayList<IntWritable>();
	    values1.add(new IntWritable(1)); 
	    reduceDriver.withInput(new Text("hi"), values1);
	    
	    List<IntWritable> values2= new ArrayList<IntWritable>();
	    values2.add(new IntWritable(1)); 
	    reduceDriver.withInput(new Text("how"), values2);
	    
	    List<IntWritable> values3 = new ArrayList<IntWritable>();
	    values3.add(new IntWritable(1)); 
	    values3.add(new IntWritable(1)); 
	    reduceDriver.withInput(new Text("you"), values3);	    
	   
	    reduceDriver.withOutput(new Text("are"), new IntWritable(1));
	    reduceDriver.withOutput(new Text("hi"), new IntWritable(1));
	    reduceDriver.withOutput(new Text("how"), new IntWritable(1));
	    reduceDriver.withOutput(new Text("you"), new IntWritable(2));
	    reduceDriver.runTest();
	  }
	   
	  @Test
	  public void testMapReduce() throws IOException {
	    mapReduceDriver.withInput(new LongWritable(), new Text("hi how are you you"));

	    mapReduceDriver.withOutput(new Text("are"), new IntWritable(1));
	    mapReduceDriver.withOutput(new Text("hi"), new IntWritable(1));
	    mapReduceDriver.withOutput(new Text("how"), new IntWritable(1));
	    mapReduceDriver.withOutput(new Text("you"), new IntWritable(2));
	    mapReduceDriver.runTest();
	  }
}
