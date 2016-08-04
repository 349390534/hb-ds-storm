package com.howbuy.storm.test;

import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;

public class DateTest {

	public static void main(String[] args) {
		while(true){
			
			
			Calendar now =Calendar.getInstance();
			/*now.set(Calendar.SECOND, 0);
		now.set(Calendar.MILLISECOND, 0);
		DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		 java.util.Date date = now.getTime();	            
		 String mystrdate = df.format(date);	           
		 Timestamp timestamp = java.sql.Timestamp.valueOf(mystrdate);
		 System.out.println(timestamp.toLocaleString());*/
			now.set(Calendar.SECOND, now.get(Calendar.SECOND)+1);
			now.set(Calendar.MILLISECOND, now.get(Calendar.MILLISECOND)+1);
			System.out.println(now.getTime().toLocaleString());
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		 

	}

}
