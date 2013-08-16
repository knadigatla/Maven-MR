package com.clodwick.mr.distcache;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * @author Kiran
 *
 */
public class JoinReducer extends Reducer<IntWritable, Text, Text, Text> {
	
	private HashMap<Integer,String> deptTable;
	
	@Override
	protected void setup(Context context)
			throws IOException, InterruptedException {
		deptTable = new HashMap<Integer, String>();
		
		String[] fields;
		String s = null;
		
		File f = new File("dept");
		if(f.exists() && f.length() > 0){
			BufferedReader br = new BufferedReader(new FileReader(f));
			while ((s=br.readLine()) != null) {
				fields = s.split(",");
				System.out.println("the values before writing"+Integer.parseInt(fields[0])+" "+ fields[1]);
				deptTable.put(Integer.parseInt(fields[0]), fields[1]);
			}
			br.close();
		}
		
		System.out.println(Arrays.toString(deptTable.keySet().toArray()));


	}
	

	@Override
	protected void reduce(IntWritable key, Iterable<Text> valList,Context context)
			throws IOException, InterruptedException {

		String[] fields = null;
		String deptName="";
//		System.out.println("the size of the hash map is "+deptTable.size());
		
		deptName = deptTable.get(key.get());
		//use key to get the deptName
		System.out.println("The department name(read):"+deptName);
		for (Text val:valList) {
//			fields = val.toString().split(",");
//			noOfFields = fields.length;
			
//			System.out.println("Writing conent into context reducer val :"+val.toString()+","+deptName);
			context.write(null, new Text(val.toString()+","+deptName));
			
		}
	}
	
	
}
