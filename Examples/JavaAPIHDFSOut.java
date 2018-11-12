import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

// this program can be executed from outside of the hadoop cluster
// convert this program into jar and run file:  $ yarn jar job.jar JavaAPIHDFS
//  $ hdfs dfs -ls
public class JavaAPIHDFSOut {
	public static void main(String[] args) throws IOException {
			Configuration conf = new Configuration();	// get current configuration of HDFS
			conf.addResource(new Path("/usr/local/hadoop/etc/hadoop/conf/core-site.xml"));
			conf.addResource(new Path("/usr/local/hadoop/etc/hadoop/conf/hdfs-site.xml"));
			conf.set("fs.defaultFS", "hdfs://10.100.52.145:10001");
			FileSystem fs = FileSystem.get(conf);		// get a handle to manage HDFS
			System.out.println(fs.getUri());   			// displays the root location with IP and port num
			Path file = new Path("/input.txt");  
			/*FileStatus fileStatus = fs.getFileStatus(file);
			BlockLocation[] blocks = fs.getFileBlockLocations(fileStatus, 0, fileStatus.getLen());*/
			
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
 