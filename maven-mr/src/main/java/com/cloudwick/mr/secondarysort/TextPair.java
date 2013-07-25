package com.cloudwick.mr.secondarysort;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class TextPair implements WritableComparable<TextPair>{

	private Text deptId;
	private IntWritable tabIndex;
	
	public TextPair() {
		this.deptId = new Text();
		this.tabIndex = new IntWritable();
	}
	
	public TextPair(Text deptId, IntWritable tabIndex) {
		this.deptId = deptId;
		this.tabIndex = tabIndex;
	}
	
	public Text getDeptId() {
		return deptId;
	}

	public void setDeptId(Text deptId) {
		this.deptId = deptId;
	}

	public IntWritable getTabIndex() {
		return tabIndex;
	}

	public void setTabIndex(IntWritable tabIndex) {
		this.tabIndex = tabIndex;
	}

	//de-serializing the object from di
	public void readFields(DataInput di) throws IOException {
//		deptId = new Text(di.readLine());
//		tabIndex = new IntWritable(di.readInt());
		deptId.readFields(di);
		tabIndex.readFields(di);
	}

	/*
	 * Serialize the fields of this object to dio.
	 * Parameters:
	 * out DataOuput to serialize this object into.
	 * Throws:
	 * java.io.IOException
	 */
	public void write(DataOutput dio) throws IOException {
//		dio.writeBytes(deptId.toString());
//		dio.write(tabIndex.get());
		deptId.write(dio);
		tabIndex.write(dio);
	}

	public int compareTo(TextPair tp) {
		if(getDeptId().compareTo(tp.getDeptId()) == 0)
			return getTabIndex().compareTo(tp.getTabIndex());
		else
			return getDeptId().compareTo(tp.getDeptId());
	}

	@Override
	public int hashCode() {
		return getDeptId().hashCode();
	}
	public int compareDeptId2(TextPair tp) {
		return getDeptId().compareTo(tp.getDeptId());
	}
}
