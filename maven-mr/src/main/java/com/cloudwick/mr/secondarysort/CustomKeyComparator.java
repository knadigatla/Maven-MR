package com.cloudwick.mr.secondarysort;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class CustomKeyComparator extends WritableComparator {

	protected CustomKeyComparator() {
		super(TextPair.class, true);
	}

	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		TextPair tp1 = (TextPair)a;
		TextPair tp2 = (TextPair)b;

		return tp1.compareTo(tp2);
	}
//	@Override
//	public int compare(Object a, Object b) {
//		TextPair tp1 = (TextPair)a;
//		TextPair tp2 = (TextPair)b;
//
//		return tp1.compareTo(tp2);	
//	}

}
