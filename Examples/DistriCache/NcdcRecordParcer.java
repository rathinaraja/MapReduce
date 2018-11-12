package DistriCache;
import org.apache.hadoop.io.Text;

public class NcdcRecordParcer {
	private String year;
	private String StationID;
	private String quality;		
	private int airTemparature;	
	private static final int MISSING_TEMPARATURE = 9999;
	
	public void parse(String record){
		StationID = record.substring(0,3);
		year = record.substring(4,8);
		String airTemparatureString = record.substring(9,11);
		airTemparature = Integer.parseInt(airTemparatureString);
		quality = record.substring(12).trim();
	}
	
	public void parse(Text record){
		parse(record.toString());
	}
	
	public boolean isValidTemparature(){
		return airTemparature != MISSING_TEMPARATURE && quality.matches("[01459]");
	}
	
	public String getStationId(){
		return StationID; 
	}
	
	public String getYear(){
		return year;
	}
	
	public int getAirTemparature(){
		return airTemparature;
	}
}
