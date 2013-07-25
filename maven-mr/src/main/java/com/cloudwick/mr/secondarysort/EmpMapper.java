package com.cloudwick.mr.secondarysort;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class EmpMapper extends Mapper<LongWritable, Text, TextPair, Text> {

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String[] fields = value.toString().split(",");
		int noOfFields = fields.length;
		
		if(noOfFields == 8) {
			context.write(new TextPair(new Text(fields[7]),new IntWritable(1)), 
					new Text(fields[0]+","+fields[1]));
		}
		
	}
}
