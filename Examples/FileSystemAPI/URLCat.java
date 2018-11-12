package FileSystemAPI;
import java.io.InputStream;
import java.net.URL;
import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;
import org.apache.hadoop.io.IOUtils;

// Displaying files from a Hadoop filesystem on standard output using a URLStreamHandler
// here hadoop doesnt understand java input stream so we need to use FsUrlStreamHandlerFactory
// $ yarn jar job.jar FileSystemAPI.URLCat hdfs://node4:10001/input.txt

public class URLCat {
	static {
		URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());
	}
	public static void main(String[] args) throws Exception {
		InputStream in = null;
		try {
			in = new URL(args[0]).openStream();
			IOUtils.copyBytes(in, System.out, 4096, false);
		} finally {
			IOUtils.closeStream(in);
		}
	}
}
