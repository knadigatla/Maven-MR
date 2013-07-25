/**
 * 
 */
package com.cloudwick.mr.empfilter;


import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class FilterMapper extends Mapper<LongWritable, Text, Text, Text> {

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String[] fields = value.toString().split(",");
		int noOfFields = fields.length;
		if(noOfFields == 8 && StringUtils.isNumeric(fields[5]) && 
				Integer.parseInt(fields[5]) > 2500) {
			context.write(null, new Text(value));
		}
		
		
	}
}
