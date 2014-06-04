package com.cloudwick.hadoop.pract;
/*
package com.comcast.cable.udf;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

public class MerlinUrlIdExtract extends UDF {
 */
import org.apache.hadoop.io.Text;

public class HiveUDF {

	/**
	 * @param args
	 */
	public Text evaluate(Text inputText) {

		String finalString = "";

		if (inputText != null) {
			String id = "";
			String inputStr = inputText.toString();
			String[] url = inputStr.split(",");
			for (int index = 0; index < url.length; index++) {
				id = url[index].substring(url[index].lastIndexOf('/') + 1,
						url[index].length()).trim();
				if(finalString == "")
					finalString = id;
				else
					finalString = finalString + "," + id;

			}
		}
		return new Text(finalString);
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		HiveUDF obj1 = new HiveUDF();
		Text str = obj1
				.evaluate(new Text(
						"http://ccpmer-po-v003-p.po.ccp.cable.comcast.com:9003/linearDataService/data/Station/4731353574791455117,,http://ccpmer-po-v003-p.po.ccp.cable.comcast.com:9003/linearDataService/data/Station/4731353574791455117,,http://ccpmer-po-v003-p.po.ccp.cable.comcast.com:9003/linearDataService/data/Station/4731353574791455117,,http://ccpmer-po-v003-p.po.ccp.cable.comcast.com:9003/linearDataService/data/Station/4731353574791455117,,http://ccpmer-po-v003-p.po.ccp.cable.comcast.com:9003/linearDataService/data/Station/4731353574791455117,,http://ccpmer-po-v003-p.po.ccp.cable.comcast.com:9003/linearDataService/data/Station/4731353574791455117,,http://ccpmer-po-v003-p.po.ccp.cable.comcast.com:9003/linearDataService/data/Station/4731353574791455117,"));

		System.out.println("hello");
		System.out.println(str.toString());

	}

}