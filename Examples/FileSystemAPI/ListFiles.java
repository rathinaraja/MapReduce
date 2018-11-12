package FileSystemAPI;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;

public class ListFiles {
	public static void main(String args[])throws Exception{
		//pass "src/cards/" as command line arguments. 
		String uri="hdfs://node2:10001/"; //args[0];
		Configuration conf=new Configuration();	
		   
		//convert path string from URI so that can manipulate with many inbuilt functions
		FileSystem fs=FileSystem.get(URI.create(uri),conf);
		Path path=new Path(uri);
		
		FileStatus[] status=fs.listStatus(path);  // to show list of files in a directory
		Path[] paths=FileUtil.stat2Paths(status);
		for(Path p:paths){
			System.out.println(p.toString()); 
		}	
		
	}
}
