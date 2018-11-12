package FileSystemAPI;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;

//dont execute this program from eclispe on windows as permission error will be the result
//execute this program in hadoop environment or eclipse on hadoop

public class FileDelete {

	public static void main(String[] args)throws IOException {
		// TODO Auto-generated method stub
		Configuration conf=new Configuration();
		String uri="hdfs://node2:10001/";
		Path path = new Path(uri); 
		FileSystem fs=FileSystem.get(URI.create(uri),conf);
		
		//display files and directories
		FileStatus[] status=fs.listStatus(path);  // to show list of files in a directory
		Path[] paths=FileUtil.stat2Paths(status);
		for(Path p:paths){
			System.out.println(p.toString()); 
		}	
		//delete input.txt
		Path path1 = new Path(uri+"/input.txt"); 
		path.getFileSystem(conf).delete(path1); 
		
		Path path2 = new Path(uri); 
		FileStatus[] status1=fs.listStatus(path2);  // to show list of files in a directory
		Path[] paths1=FileUtil.stat2Paths(status);
		//display files and directories
		for(Path p:paths1){
			System.out.println(p.toString()); 
		}	
	}

}
