package com.cloudwick.mr.secondarysort;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class SSDriver extends Configured implements Tool{

	public int run(String[] args) throws Exception {
		if(args.length != 3) {
			System.out.printf("Usage: %s [generic options] <input dir> <output dir>\n", 
					getClass().getSimpleName());
			ToolRunner.printGenericCommandUsage(System.out);
            return -1;
		}
		
		Job job = new Job(getConf());
		job.setJarByClass(SSDriver.class);
		job.setJobName(this.getClass().getName());
		
		MultipleInputs.addInputPath(job, new Path(args[0]), 
				TextInputFormat.class, EmpMapper.class);
		MultipleInputs.addInputPath(job, new Path(args[1]), 
				TextInputFormat.class, DeptMapper.class);
		FileOutputFormat.setOutputPath(job, new Path(args[2]));
		job.setReducerClass(SSReducer.class);

		
		job.setMapOutputKeyClass(TextPair.class);
		job.setMapOutputValueClass(Text.class);
		
		
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		
		job.setPartitionerClass(CustomPartitioner.class);
//		job.setSortComparatorClass(CustomKeyComparator.class);
		job.setGroupingComparatorClass(CustomGroupComparator.class);
		
		if(job.waitForCompletion(true)) {
			return 0;
		}
		return 1;
	}

	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new SSDriver(), args));
	}
}
