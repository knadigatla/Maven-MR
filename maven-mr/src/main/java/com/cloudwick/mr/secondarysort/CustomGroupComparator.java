package com.cloudwick.mr.secondarysort;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class CustomGroupComparator extends WritableComparator {

	protected CustomGroupComparator() {
		super(TextPair.class, true);
		// TODO Auto-generated constructor stub
	}
	
	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		TextPair tp1 = (TextPair)a;
		TextPair tp2 = (TextPair)b;

		return tp1.compareDeptId2(tp2);
	}

}
