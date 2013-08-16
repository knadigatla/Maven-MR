package com.clodwick.mr.distcache;
/**
 * 
 */
import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class JoinMapper extends Mapper<LongWritable, Text, IntWritable, Text> {

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String[] fields = value.toString().split(",");
		int noOfFields = fields.length;
		String finalVal = null;
		
		if(noOfFields == 8 && StringUtils.isNumeric(fields[7])) {
			finalVal = fields[0]+","+fields[1];
			context.write(new IntWritable(Integer.parseInt(fields[7])), new Text(finalVal));
		}
		
		
	}
}
