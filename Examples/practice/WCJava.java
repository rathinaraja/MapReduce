package practice;


import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class WCJava {
	
	public static Map<String, Integer> wordCount(String strings) {
		Map<String, Integer> map = new HashMap<String, Integer> ();
		String[] splitted = strings.split(" ");
		for (String s:splitted) { 
		    if (!map.containsKey(s)) {  // first time we've seen this string
		      map.put(s, 1);
		    }
		    else {
		      int count = map.get(s);
		      map.put(s, count + 1);
		    }
		 }
		 return map;
	}
	
	public static void main(String ar[]) throws IOException{
		System.out.println("enter a paragraph");
		BufferedReader br=new BufferedReader (new InputStreamReader(System.in));
		String str=br.readLine();
		Map<String, Integer> result=wordCount(str);
		System.out.println(Arrays.asList(result));
	}

}

