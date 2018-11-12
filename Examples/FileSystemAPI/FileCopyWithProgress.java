package FileSystemAPI;

import java.io.*;
import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;

//by printing a period after each 64 KB packet of data is written to the datanode pipeline
public class FileCopyWithProgress {
	public static void main(String[] args) throws Exception {
		String targeturi="hdfs://node2:10001/filename.txt";
		String localuri = "input.txt";	// current directory 
		InputStream in = new BufferedInputStream(new FileInputStream(localuri));
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(targeturi), conf);
		OutputStream out = fs.create(new Path(targeturi), new Progressable(){
			public void progress() {
				System.out.print(".");
			}
		});
		IOUtils.copyBytes(in, out, 4096, true);
	}
}
