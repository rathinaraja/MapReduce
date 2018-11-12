package CustomKeyValue;

import org.apache.hadoop.io.*;

public class MultiWritableValue extends GenericWritable{
	
	private static Class[] CLASSES = new Class[]{
			IntWritable.class,
			Text.class
	};
	
	public MultiWritableValue(){
	}
	
	public MultiWritableValue(Writable value){
		set(value);
	}
	
	protected Class[] getTypes() {
		return CLASSES;
	}
}
