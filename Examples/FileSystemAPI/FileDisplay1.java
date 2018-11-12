package FileSystemAPI;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI; 

import org.apache.hadoop.conf.Configuration; 
import org.apache.hadoop.fs.FileSystem; 
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

public class FileDisplay1 {

	public static void main(String[] args) throws IOException { 
			String uri="hdfs://node2:10001/input.txt"; //args[0];
			Configuration conf=new Configuration();	
				   
			//convert path string from URI so that can manipulate with many inbuilt functions
			FileSystem fs=FileSystem.get(URI.create(uri),conf);
			InputStream in=null; 
			try{
				in=fs.open(new Path(uri));
				IOUtils.copyBytes(in, System.out, 4096,false);
				//find how to use seek method
			}
			finally{
				IOUtils.closeStream(in);
			}	
	}

}
