import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;


// this program msut be executed in any one of the hadoop installed machines
// convert this program into jar and run file:  $ yarn jar job.jar JavaAPIHDFS
//  $ hdfs dfs -ls

public class JavaAPIHDFS {
	public static void main(String[] args) throws IOException, URISyntaxException {
			Configuration conf = new Configuration();	// get current configuration of HDFS
			FileSystem fs = FileSystem.get(conf);		// get a handle to manage HDFS
			System.out.println(fs.getUri());   			// displays the root location with IP and port num
			
			Path outputPath = new Path("intermediate_output");
		     FileSystem  fs1 = FileSystem.get(new URI(outputPath.toString()), conf);
		        //It will delete the output directory if it already exists. don't need to delete it  manually  
		     fs.delete(outputPath);
			Path file = new Path("demo.txt");  			// creates file in hadoop home directory
			if (fs.exists(file)) {
				System.out.println("File exists.");
			}
			else {
				// Writing to file under username as we didn specify any path here /user/itadmin/..
				FSDataOutputStream outStream = fs.create(file);		// fs object creates a file and gives handle to to output stream
				outStream.writeUTF("Welcome to HDFS Java API!!!");	// reading and writing the primitive data types , not objective types
				outStream.close();
			}
			// Reading from file
			FSDataInputStream inStream = fs.open(file);
			String data = inStream.readUTF();
			System.out.println(data);
			inStream.close();
			fs.close();			
	}
}