package Filtering;

import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*; 
import NYSE.CustomKeyValue.*;
import NYSE.Parsers.*;

public class AvgStockMapperWithProperties extends Mapper<LongWritable,Text,TextPair,LongPair>{
		
	private static NYSEParser parser=new NYSEParser();	
	private static TextPair mapoutputkey=new TextPair();
	private static LongPair mapoutputvalue=new LongPair();
	
	public void map(LongWritable key, Text record, Context context)throws IOException, InterruptedException{
		parser.parse(record.toString());   
		
		//option1: hardcode the stock ticker name
		//if(parser.getStockTicker().equals("BAC")){
		
		//option2: everytime we have to remodify stock tickername from BAC to other and then convert to jar and run job. 
		//so, using property can pass -Dfilter.by.stockTicker=BAC while launching job
		if (parser.getStockTicker().equals(context.getConfiguration().get("filter.by.stockTicker"))){   // you can filter based on any stock ticker
			// set mapoutput key (trademonth + stock ticker)
			mapoutputkey.setFirst(new Text(parser.getTradeMonth()));
			mapoutputkey.setSecond(new Text(parser.getStockTicker()));
			
			// set mapoutput value (volume + integer 1) 
			mapoutputvalue.setFirst(new LongWritable(parser.getVolume()));
			mapoutputvalue.setSecond(new LongWritable(1)); 		// this is to find number of records in NYSE files
					
			context.write(mapoutputkey,mapoutputvalue);
			// 2014-01	RBS-H	25800	1
		}
	}
}
