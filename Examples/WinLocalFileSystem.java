
import java.io.IOException;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;

public class WinLocalFileSystem extends LocalFileSystem {
	public WinLocalFileSystem() {
		super();
		System.err.println("Patch for HADOOP-7682: "+"Instantiating workaround file system");
	}
 
	@Override
	public boolean mkdirs(Path path, FsPermission permission)
			throws IOException {
		boolean result=super.mkdirs(path);
		this.setPermission(path,permission);
		return result;
	}

	@Override
	public void setPermission(Path path, FsPermission permission)
			throws IOException {
		try {
			super.setPermission(path,permission);
		}
		catch (IOException e) {
			System.err.println("Patch for HADOOP-7682: "+
				"Ignoring IOException setting persmission for path \""+path+
				"\": "+e.getMessage());
		}
	}
}
