package DistriCache;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.fs.Path;

public class NcdcStationMetadataParcer {
	private String stationMetadata; 
	private String stationName;
	private HashMap<String,String> hm = new HashMap<String,String>();
	
	public String getStationName(String key){
	    // Get a set of the entries
	    Set set = hm.entrySet();
	    // Get an iterator
	    Iterator itr = set.iterator();
	    // Get the key,value for an elements in the HashMap
	    while(itr.hasNext()) 
	    {	    	
	    Map.Entry<String,String> me = (Map.Entry)itr.next();
	         if (me.getKey().compareTo(key)==0){
	        	stationName = me.getValue();
	        	break;
	         }
	    }		
		return stationName;
	}
	public void initialize(Path path) throws IOException{
		FileReader fr = new FileReader(path.toString());
		BufferedReader buff = new BufferedReader(fr);
		while((stationMetadata = buff.readLine()) != null){
			String s[] =stationMetadata.split(" ");
			String key = s[0];
			String value = s[1];
				if (key !=" " && value != " "){
					hm.put(key, value);
				}			
		}		
	}	
}
