package com.howbuy.storm.test;

import java.util.Calendar;
import java.util.Date;

import com.howbuy.storm.common.HbConstants;

public class DataTest {

	public static void main(String[] args) {
	/*	Map<String,Integer> openAccMap = new HashMap<String, Integer>();
		String key ="kkey";
		for(int i=0;i<5;i++){
			Integer acNum = openAccMap.get(key);
			if(null == acNum)
				acNum = new Integer(0);
			acNum++;
			openAccMap.put(key, acNum);
			System.out.println(acNum);
		}
		System.out.println(openAccMap.get(key));*/
		/*Date d01 = new Date(1447173574914l);
		System.out.println(d01);
		Date d02 = new Date(1447173428419l);
		System.out.println(d02);
		System.out.println("----------");
		Date d = new Date(1447244658214l);
		System.out.println(d);
		Date d1 = new Date(1447244726620l);
		System.out.println(d1);
		Date d2 = new Date(1447244748165l);
		System.out.println(d2);
		Calendar c = Calendar.getInstance();
		c.set(Calendar.DAY_OF_MONTH, 16);
		c.set(Calendar.SECOND, 0);
		c.set(Calendar.HOUR_OF_DAY, 20);
		c.set(Calendar.MINUTE, 20);
		System.out.println(c.getTime().toLocaleString());
		System.out.println(c.getTimeInMillis());*/
		String data= "1470156411809^A6^A5000^A000217^Anull^Anull^Anull^Anull^A1^A1^A1406381623^Anull^Anull^AA20160113^AHD0001^Ad6bd417e-83ca-4644-b935-343adc65c203^Anull^Anull^Anull^A105975974^Anull";
		String[] as = data.split(HbConstants.FIELD_SEPARATOR);
		System.out.println(as[19]);
	}

}
