package Filtering;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;  
import NYSE.CustomKeyValue.*;
import NYSE.Parsers.*;

public class AvgStockMapperWithSetUp extends Mapper<LongWritable,Text,TextPair,LongPair>{
		
	private static NYSEParser parser=new NYSEParser();	
	private static TextPair mapoutputkey=new TextPair();
	private static LongPair mapoutputvalue=new LongPair();
	private static String stockTicker;
	private Set<String> stockTickers=new HashSet<String>();			// Set is easy to lookup
	
	 
	public void setup(Context context)throws IOException, InterruptedException{
		String[] tickers=null;						// to get multiple stock tickers
		stockTicker=context.getConfiguration().get("filter.by.stockTicker")	;
		if(stockTicker!=null){
			tickers=stockTicker.split(","); 		//-Dfilter.by.stockTicker=BAC,AEO 
		}	
		for(String tick:tickers){
			stockTickers.add(tick);
		}
	}
	
	public void map(LongWritable key, Text record, Context context)throws IOException, InterruptedException{
		parser.parse(record.toString());  // as we receive there as string	 
		
		//if(parser.getStockTicker().equals(stockTicker)){
		//if (parser.getStockTicker().equals(context.getConfiguration().get("filter.by.stockTicker"))){  
		if(stockTickers.isEmpty()||stockTickers.contains(parser.getStockTicker())){
			// set mapoutput key (trademonth + stock ticker)
			mapoutputkey.setFirst(new Text(parser.getTradeMonth()));
			mapoutputkey.setSecond(new Text(parser.getStockTicker()));
				
			// set mapoutput value (volume + integer 1) 
			mapoutputvalue.setFirst(new LongWritable(parser.getVolume()));
			mapoutputvalue.setSecond(new LongWritable(1)); // this is to find number of records in NYSE files
						
			context.write(mapoutputkey,mapoutputvalue);
			// 2014-01	RBS-H	25800	1
		}
	}
}
