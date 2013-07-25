package com.cloudwick.mr.empjoin;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * @author Kiran
 *
 */
public class JoinReducer extends Reducer<IntWritable, Text, Text, Text> {

	@Override
	protected void reduce(IntWritable key, Iterable<Text> valList,Context context)
			throws IOException, InterruptedException {

		String[] fields = null;
		int noOfFields;
		String deptName="";
		for (Text val:valList) {
			fields = val.toString().split(",");
			noOfFields = fields.length;
			
			if(noOfFields == 1) {
				deptName = fields[0];
				break;
			}
			
		}
		
		System.out.println("The department name(red):"+deptName);
		for (Text val:valList) {
			fields = val.toString().split(",");
			noOfFields = fields.length;
			
			if(noOfFields == 2) {
				System.out.println("Writing conent into context reducer val :"+val.toString()+","+deptName);
				context.write(null, new Text(val.toString()+","+deptName));

			}
			
		}
	}
	
	
}
