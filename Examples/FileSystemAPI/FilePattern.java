 package FileSystemAPI;

 import java.net.URI;
 import org.apache.hadoop.conf.Configuration;
 import org.apache.hadoop.fs.FileStatus;
 import org.apache.hadoop.fs.FileSystem;
 import org.apache.hadoop.fs.FileUtil;
 import org.apache.hadoop.fs.Path;

 public class  FilePattern  {
 	public static void main(String args[])throws Exception{    
 		String uri=args[0];
 		Configuration conf=new Configuration();	
 		
 		//convert path string from URI so that can manipulate with many inbuilt functions
 		FileSystem fs=FileSystem.get(URI.create(uri),conf);
 		//Path path=new Path(args[0]+"/*");   	
 		//Path path=new Path(args[0]+"*/BySuite.java"); 
 		//Path path=new Path(args[0]+"/nyse_201?.*");
 		Path path=new Path(args[0]+"/nyse_201[1-2].*");
 		
 		FileStatus[] status=fs.globStatus(path);
 		Path[] paths=FileUtil.stat2Paths(status);
 		for(Path p:paths){
 			System.out.println(p.toString()); 
 		}
 	}
 }
