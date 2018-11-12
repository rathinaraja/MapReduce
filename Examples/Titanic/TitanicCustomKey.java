package Titanic;

import java.io.DataInput;
import java.io.DataOutput; 
import java.io.IOException; 
import org.apache.hadoop.io.*; 
import com.google.common.collect.ComparisonChain;
//class for special comparison on collection objects

public class TitanicCustomKey implements WritableComparable<TitanicCustomKey> {
 
	private String x; 
	private String y;
 
	public TitanicCustomKey(){ 
	}
	
	public TitanicCustomKey(String x, String y) { 
		this.x = x; 
		this.y = y; 
	}
	
	public void readFields(DataInput in) throws IOException { 
		x = in.readUTF(); 
		y = in.readUTF(); 
	}
	
	public void write(DataOutput out) throws IOException { 
		out.writeUTF(x); 
		out.writeUTF(y); 
	} 
	
	/*
	 In the compareTo method, we have written our logic to sort the keys by the gender column. 
	 ComparisionChain class compares gender column and then compares the 1st column. 
	 Therefore, this logic will print the keys sorted by Gender column.
	 Note: If you compare only one column, then the second will be considered as a single value by the 
	 WritableComparable interface.
	*/
	@Override
	public int compareTo(TitanicCustomKey o) {		 
		// TODO Auto-generated method stub		 
		return ComparisonChain.start().compare(this.y,o.y).compare(this.x,o.x).result();		 
	}
		
	public boolean equals(Object o1) { 
		if (!(o1 instanceof TitanicCustomKey)) { 
			return false; 
		} 
		TitanicCustomKey other = (TitanicCustomKey)o1; 
		return this.x == other.x && this.y == other.y;
	}
 
	@Override 
	public String toString() { 
		return x.toString()+","+y.toString(); 
	} 
	
	public String getX() {
		return x; 
	}
 
	public void setX(String x) { 
		this.x = x; 
	}
 
	public String getY() { 
		return y; 
	}
 
	public void setY(String y) {
		this.y = y; 
	} 
}
