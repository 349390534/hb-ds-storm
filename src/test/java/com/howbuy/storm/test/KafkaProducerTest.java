/**
 * 
 */
package com.howbuy.storm.test;

import java.util.Date;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.howbuy.storm.common.HbConstants;

/**
 * @author qiankun.li
 *
 */
public class KafkaProducerTest {
	private static Logger logger = LoggerFactory.getLogger(KafkaProducerTest.class);
	
	private static String pageid(String pageId){
		final StringBuffer sb = new StringBuffer();
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
		sb.append("3504881008080101"+pageId);
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
		sb.append("null");
		return sb.toString();
	}
	public static void main(String[] args) {
		
		for(int i=0;i<=3;i++){
			new Thread(){
				int count =0;
				final String topic = "storm_web_news";
				KafkaProducer producer = new KafkaProducer(topic);
				String[] pageidArray = {"0003","1113","2223","3333","4443","5553","6663","7773","8883","9993"};
				
				Random randon = new Random();
				
				@Override
				public void run() {
					String key = Thread.currentThread().getName();
					while(true){

						
						if(count==20){
//							System.exit(0);
							return;
						}
					
						int i = new Random().nextInt(5);
						String message =pageid(pageidArray[i]);
						producer.pubMessage(key+Math.random(), message);
						
						System.out.println(
								Thread.currentThread().getId() + " produce : " + ++count);
						
						try {
							
							Thread.sleep(randon.nextInt(1500));
							
						} catch (InterruptedException e) {
							logger.error(key+"休眠异常",e);
						}
					}
				}
				
			}.start();
		}
	

	}
}
