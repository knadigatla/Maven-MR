package com.cloudwick.hadoop.pract;

import java.util.Arrays;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;

public class Temp {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		String k = "Kiran";
		
		byte[] x = k.getBytes();
		
		
		BytesWritable bw = new BytesWritable(k.getBytes());
		String y = new String(bw.getBytes());
		Text t = new Text(bw.getBytes());

		System.out.println(t);

	}

}
