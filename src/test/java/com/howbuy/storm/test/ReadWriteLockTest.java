/**
 * 
 */
package com.howbuy.storm.test;

import java.util.Random;

import com.howbuy.storm.common.HbConstants;
import com.howbuy.storm.job.WebNewsPvJob;

/**
 * @author qiankun.li
 *
 */
public class ReadWriteLockTest {

	public static void main(String[] args) {
		
		
		/*for(int i=0;i<5;i++){
			new Thread(){
				@Override
				public void run() {
					 WebNewsPvJob.getJob().putIntoQueue("data:"+System.currentTimeMillis()+"-");
				}
				
			}.start();
			try {
				Thread.sleep(100);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}*/
		
		/*String str = "null	null	NULL	NULL	NULL	NULL	NULL	2015-08-01	web";
		String[] a = str.split(HbConstants.FIELD_SEPARATOR);
		System.out.println(a);*/
		
		if(!WebNewsPvJob.getJob().isStart())
			WebNewsPvJob.getJob().excute();
		new Thread(){
			StringBuffer sb = new StringBuffer();
			{
			sb.append("0xffba362e6c61e5f2");
			sb.append(HbConstants.FIELD_SEPARATOR);
			sb.append("1438358404962");
			sb.append(HbConstants.FIELD_SEPARATOR);
			sb.append("null");
			sb.append(HbConstants.FIELD_SEPARATOR);
			sb.append("null");
			sb.append(HbConstants.FIELD_SEPARATOR);
			sb.append("Windows	IE6");
			sb.append(HbConstants.FIELD_SEPARATOR);
			sb.append("124.231.98.103");
			sb.append(HbConstants.FIELD_SEPARATOR);
			sb.append("null");
			sb.append(HbConstants.FIELD_SEPARATOR);
			sb.append("null");
			sb.append(HbConstants.FIELD_SEPARATOR);
			sb.append("null");
			sb.append(HbConstants.FIELD_SEPARATOR);
			sb.append("http://www.howbuy.com/");
			sb.append(HbConstants.FIELD_SEPARATOR);
			sb.append("website");
			sb.append(HbConstants.FIELD_SEPARATOR);
			sb.append("index");
			sb.append(HbConstants.FIELD_SEPARATOR);
			sb.append("-1");
			sb.append(HbConstants.FIELD_SEPARATOR);
			sb.append("1");
			sb.append(HbConstants.FIELD_SEPARATOR);
			sb.append("null");
			sb.append(HbConstants.FIELD_SEPARATOR);
			sb.append("null");
			sb.append(HbConstants.FIELD_SEPARATOR);
			sb.append("null");
			sb.append(HbConstants.FIELD_SEPARATOR);
			sb.append("null");
			sb.append(HbConstants.FIELD_SEPARATOR);
			sb.append("35048810080801013153");
			sb.append(HbConstants.FIELD_SEPARATOR);
			sb.append("null");
			sb.append(HbConstants.FIELD_SEPARATOR);
			sb.append("null");
			sb.append(HbConstants.FIELD_SEPARATOR);
			sb.append("null");
			sb.append(HbConstants.FIELD_SEPARATOR);
			sb.append("null");
			sb.append(HbConstants.FIELD_SEPARATOR);
			sb.append("null");
			sb.append(HbConstants.FIELD_SEPARATOR);
			sb.append("null");
			sb.append(HbConstants.FIELD_SEPARATOR);
			sb.append("null");
			sb.append(HbConstants.FIELD_SEPARATOR);
			sb.append("null");}
			@Override
			public void run() {
				while(true){
					/*new Thread(){
						int  num = 10;
						@Override
						public void run() {}
					}.start();*/

					
					for(int i =0 ;i<5;i++){
						long t = System.currentTimeMillis();
						System.out.println("添加数据:"+sb.toString());
						WebNewsPvJob.getJob().putIntoQueue(sb.toString());
						try {
							Thread.sleep(10);
						} catch (InterruptedException e) {
							e.printStackTrace();
						}
					}
				
					try {
						Thread.sleep(1000);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
			}
			
		}.start();
	
		
	}
}
