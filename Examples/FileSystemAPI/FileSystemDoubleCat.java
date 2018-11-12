package FileSystemAPI;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

// Displaying files from a Hadoop filesystem on standard output twice, by using seek()
// $ yarn jar job.jar FileSystemAPI.FileSystemDoubleCat hdfs://node4:10001/input.txt

public class FileSystemDoubleCat {
	public static void main(String[] args) throws Exception {
		String uri = args[0];
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(uri), conf);
		FSDataInputStream in = null;
		try {
			in = fs.open(new Path(uri));
			IOUtils.copyBytes(in, System.out, 4096, false);
			in.seek(0); // go back to the start of the file
			IOUtils.copyBytes(in, System.out, 4096, false);
		} finally {
			IOUtils.closeStream(in);
		}
		}
}
