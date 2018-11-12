package CustomKeyValue;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.*;
//data set is log data set
/*data type implementing WritableComparable interface can behave as key and value as well. we are going to write a progam
to find occurances of IP address in a server log. it can be done by tokenizing text and assigning 1 using normal WC program
howevr, implmenting a key comprising set of keys, that can control which field to compare and sort in intermediate process
unlike normal WC programs. 
try using CustomKey as value 
for just to use as value data type enough using constructor readFields() and write() methods
if you want to use this as key data type then you have to include extra hashkey, compareto and equals methods as it 
is required for comparing in magical phsase
you need use hashCode() method as partitioner distributes key to the reducers using hashkeypartitioner by default */

/*                                 reNo      URL            regDate     time        IP
   data set containng five fields: 56682187	/vfnlyvnx.html	2014-03-10	12:22:10	191.170.95.175
                                   IntWritable Text			Text		Text		Text

we are going to create a custom data type comprising five hadoop data types. Therefore, in the custom datatype
you can access any fields you want to compare for sorting. in our example, sorting is done based on IP. if equals then it emits time*/

public class CustomKey implements WritableComparable <CustomKey>{
	
	private Text URL, reqDate, timestamp, IP;
	private IntWritable reqNo;
	
	public CustomKey(){
		this.URL=new Text();
		this.reqDate=new Text();
		this.timestamp=new Text();
		this.IP=new Text();
		this.reqNo=new IntWritable();
	}
	
	public CustomKey(Text URL, Text reqDate, Text time, Text IP, IntWritable reqNo) { 
		this.URL=URL;
		this.reqDate = reqDate;
		this.timestamp = time; 
		this.IP=IP;
		this.reqNo = reqNo;
	}
	
	public void set(Text URL, Text reqDate, Text time, Text IP, IntWritable reqNo){
		this.URL=URL;
		this.reqDate = reqDate;
		this.timestamp = time; 
		this.IP=IP;
		this.reqNo = reqNo;		
	}
	
	public Text getIP() {
		return IP;
	}
	
	@Override
	// readFieds de-serializes and read data from stream 
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		IP.readFields(in);
		timestamp.readFields(in);
		reqDate.readFields(in);
		reqNo.readFields(in);
		URL.readFields(in);
	}

	@Override
	// serrializes data before writing onto the stream
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		IP.write(out);
		timestamp.write(out);		
		reqDate.write(out);
		reqNo.write(out);
		URL.write(out);
		//System.out.println(URL +"\t"+ reqDate+ "\t"+ timestamp+ "\t"+IP+ "\t"+ reqNo);
	}

	@Override
	//sorting is done based on the IP. it returns -1 or 0 or 1
	public int compareTo(CustomKey o){
		if(IP.compareTo(o.IP)==0){
			return timestamp.compareTo(o.timestamp);
		}
		else
			return IP.compareTo(o.IP); 
	}
	
	// object is same only when iP and time are same for comparison
	public boolean equals(Object o) {
		// TODO Auto-generated method stub
		if(o instanceof CustomKey){
			CustomKey temp=(CustomKey)o;
			return  IP.equals(temp.IP) && timestamp.equals(temp.timestamp); //URL.equals(temp.URL) && regDate.equals(temp.regDate) && regNo.equals(temp.regNo);
		}
		return false;
	}
		
	/*public Text getURL() {
		return URL;
	}

	public void setURL(Text uRL) {
		URL = uRL;
	}

	public Text getRegDate() {
		return regDate;
	}

	public void setRegDate(Text regDate) {
		this.regDate = regDate;
	}

	public Text getTime() {
		return time;
	}

	public void setTime(Text time) {
		this.time = time;
	}

	public Text getIP() {
		return IP;
	}

	public void setIP(Text iP) {
		IP = iP;
	}

	public IntWritable getRegNo() {
		return regNo;
	}

	public void setRegNo(IntWritable regNo) {
		this.regNo = regNo;
	}*/
	
	public int hashCode(){
		return IP.hashCode();
	}	
}
