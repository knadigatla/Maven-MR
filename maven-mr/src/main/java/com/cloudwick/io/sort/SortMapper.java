package com.cloudwick.io.sort;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SortMapper extends Mapper<LongWritable, Text, IntWritable, Text> {

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String[] words = value.toString().split(" ");
		if (StringUtils.isNumeric(words[0])) {
			context.write(new IntWritable(Integer.parseInt(words[0])), value);
		}

	}
}
