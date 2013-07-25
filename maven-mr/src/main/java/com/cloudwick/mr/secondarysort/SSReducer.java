package com.cloudwick.mr.secondarysort;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SSReducer extends Reducer<TextPair, Text, NullWritable, Text> {

	@Override
	protected void reduce(TextPair tp, Iterable<Text> valList, Context context)
			throws IOException, InterruptedException {
		String deptName = null;
		boolean set = false;
		for( Text val:valList) {
			if(set) {
				context.write(null, new Text(val.toString()+","+deptName));
			}else {
				deptName = val.toString();
				set = true;
			}
		}
	}
}
