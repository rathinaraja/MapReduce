package FileSystemAPI;

import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;

//merging too many small files into one before passing to MR framework manually

public class CopyMerge {
	public static void main(String args[])throws Exception{
		//pass "src/cards/" as command line arguments. other paths like D:/.. and file:/// not working. check
		String uri=args[0];
		Configuration conf=new Configuration();	
		
		//convert path string from URI so that can manipulate with many inbuilt functions
		FileSystem fs=FileSystem.get(URI.create(uri),conf);
		
		//in run arguments: src/cards/ src/new.txt
		Path srcPath=new Path(args[0]);		 // it is a directory containing set of files
		Path targetPath=new Path(args[1]);   // it is a file name not directory name
		
		boolean copyMerge=FileUtil.copyMerge(fs, srcPath, fs, targetPath, false, conf, null); //false is to mean not to delete source after merging		
		if(copyMerge){
			System.out.println("merge successful");
		}
	}
}
