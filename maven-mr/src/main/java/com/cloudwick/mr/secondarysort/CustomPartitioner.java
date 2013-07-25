package com.cloudwick.mr.secondarysort;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class CustomPartitioner extends Partitioner<TextPair, Text> {

	@Override
	public int getPartition(TextPair tp, Text txt, int noOfPartitions) {

		return tp.hashCode()%noOfPartitions;
	}

}
