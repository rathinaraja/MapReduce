package FileSystemAPI;

import java.io.InputStream;
import java.net.URL;

import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;
import org.apache.hadoop.io.IOUtils;

public class FileDisplay {
	static {
		URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());
	}
	
	public static void main(String[] args)throws Exception{
		InputStream in=null;
		String uri="hdfs://node2:10001/input.txt"; //args[0];
		try{
			in=new URL(uri).openStream();
			IOUtils.copyBytes(in, System.out, 4096,false);
		}
		finally{
			IOUtils.closeStream(in);
		}
	}
}

