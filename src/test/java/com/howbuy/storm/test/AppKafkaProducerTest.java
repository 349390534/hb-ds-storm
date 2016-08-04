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
public class AppKafkaProducerTest {
	private static Logger logger = LoggerFactory.getLogger(AppKafkaProducerTest.class);
	
	static AtomicLong atomicLong = new AtomicLong();
	static AtomicLong atomicLong1 = new AtomicLong();
	static final String topic = "topic_appactivate";
	static final String topic_app = "topic_appevent";
	
	
	/**构�?APPEvent测试数据
	 * @param pageId
	 * @return
	 */
	private static String buildAPPEventData(){

		StringBuilder builder = new StringBuilder();

		builder.append(System.currentTimeMillis());
		builder.append(HbConstants.FIELD_SEPARATOR);
		String[] type = new String[]{"4","5","6","8","9","10","13","14"};
		builder.append(type[new Random().nextInt(type.length)]);//type: 事件类型�?-�?���?-绑卡�?-买基金�?7-赎回�?
		//8-储蓄罐存入�?9-储蓄罐取现�?10-定投�?1-转换�?2-撤单�?3-储蓄罐活期转定期�?4-储蓄罐直接购买定期�?15-交易账号登录�?
		builder.append(HbConstants.FIELD_SEPARATOR);
		builder.append(new Random().nextInt(200));//amount: 交易金额（元）（部分业务类型该�?为空�?
		builder.append(HbConstants.FIELD_SEPARATOR);
		builder.append("482001");//fundCode: 基金代码
		builder.append(HbConstants.FIELD_SEPARATOR);
		builder.append("1");//toFundCode
		builder.append(HbConstants.FIELD_SEPARATOR);
		builder.append("62259876543456");//BankAcct
		builder.append(HbConstants.FIELD_SEPARATOR);
		builder.append("1");//idType证件类型
		builder.append(HbConstants.FIELD_SEPARATOR);
		builder.append("63234567896543987");//IdNo
		builder.append(HbConstants.FIELD_SEPARATOR);
		builder.append("1");//xRslt
		builder.append(HbConstants.FIELD_SEPARATOR);
		builder.append("1");//payType:1-代扣 2-网银转账 （只针对交易业务才有值）
		builder.append(HbConstants.FIELD_SEPARATOR);
		builder.append("1");//custNo
		builder.append(HbConstants.FIELD_SEPARATOR);
		builder.append("1");//contractNo
		builder.append(HbConstants.FIELD_SEPARATOR);
		builder.append("110");//phone
		builder.append(HbConstants.FIELD_SEPARATOR);
		String[] coopid = new String[]{"A20120701","H20131104"};
		builder.append(coopid[new Random().nextInt(coopid.length)]);//coopId
//		builder.append("A20150128");//coopId
		builder.append(HbConstants.FIELD_SEPARATOR);
		builder.append("null");//actId
		builder.append(HbConstants.FIELD_SEPARATOR);
		builder.append("1234554321");//tokenId
		builder.append(HbConstants.FIELD_SEPARATOR);
		builder.append("null");//（APP报当前页面事件ID，H5报当前页面url�?
		builder.append(HbConstants.FIELD_SEPARATOR);
		builder.append("1");//份额（赎回，转换时使用）
		builder.append(HbConstants.FIELD_SEPARATOR);
		builder.append("null");//email
		builder.append(HbConstants.FIELD_SEPARATOR);
		String[] proid = new String[]{"28294488","105975974"};
		
//		builder.append("105975974");
		builder.append(proid[new Random().nextInt(proid.length)]);
		builder.append(HbConstants.FIELD_SEPARATOR);
		builder.append("otag");// 访问路径跟踪 otag
		return builder.toString();
	}
	
	
	/**构�?�?��数据
	 * @param pageId
	 * @return
	 */
	private static String buildActivationMes(String custno){
		final StringBuffer sb = new StringBuffer();
		sb.append(System.currentTimeMillis());
		sb.append(HbConstants.FIELD_SEPARATOR);
		sb.append("2002");//Proid
		sb.append(HbConstants.FIELD_SEPARATOR);
		sb.append("1438358404962");//Imeis
		sb.append(HbConstants.FIELD_SEPARATOR);
		sb.append("1234554321");//Guid
		sb.append(HbConstants.FIELD_SEPARATOR);
		String[] coopid = new String[]{"A20120701","H20131104","A20140301"};
		sb.append(coopid[new Random().nextInt(coopid.length)]);//coopId
//		sb.append("A20150128");//Outletcode
		sb.append(HbConstants.FIELD_SEPARATOR);
		sb.append("1");//type
		sb.append(HbConstants.FIELD_SEPARATOR);
		sb.append("90998"+custno);//Custno
		sb.append(HbConstants.FIELD_SEPARATOR);
		sb.append("1");//resolution
		sb.append(HbConstants.FIELD_SEPARATOR);
		sb.append("1");//agent
		sb.append(HbConstants.FIELD_SEPARATOR);
		sb.append("9.1");//version
		sb.append(HbConstants.FIELD_SEPARATOR);
		sb.append("wifi");//network
		sb.append(HbConstants.FIELD_SEPARATOR);
		sb.append("2.2.2");//appversion
		sb.append(HbConstants.FIELD_SEPARATOR);
		sb.append("ext");//ext
		return sb.toString();
	}
	
	
	public static void main(String[] args) {
		
		//测试�?��数据发�?
		for(int i=0;i<5;i++){
			new Thread(){
				KafkaProducer producer = new KafkaProducer(topic);
				String[] custnoArray = {"0003","1113","2223","3333","4443","5553","6663","7773","8883","9993"};
				@Override
				public void run() {
					int count = 0;
					String key = Thread.currentThread().getName();
					while(true){
							/*if(count == 50){
//								System.exit(0);
								return;
							}*/
						int i = new Random().nextInt(custnoArray.length);
						String message =buildActivationMes(custnoArray[i]);
						producer.pubMessage(key+Math.random(), message);
						atomicLong.getAndAdd(1);
						//logger.info(new Date().toLocaleString()+",线程:"+key+"已发送消息["+count+"]�?);
						//logger.info(new Date().toLocaleString()+",�?��发�?消息["+atomicLong.longValue()+"]�?);
						System.out.println(new Date().toLocaleString()+","+Thread.currentThread().getName() + " -activate:" + ++count);
						/*try {
							Thread.sleep(1000);
						} catch (InterruptedException e) {
							logger.error(key+"休眠异常",e);
						}*/
					}
				}
				
			}.start();
		}
		
		//APPEvent 测试
		for(int i=0;i<5;i++){
			new Thread(){
				KafkaProducer producer = new KafkaProducer(topic_app);
				@Override
				public void run() {
					int count = 0;
					String key = Thread.currentThread().getName();
					while(true){
						/*if(count == 50){
//							System.exit(0);
							return;
						}*/
					
						String message =buildAPPEventData();
						producer.pubMessage(key+Math.random(), message);
						
						atomicLong1.getAndAdd(1);
						//logger.info(new Date().toLocaleString()+",线程:"+key+"已发送消息["+count+"]�?);
						//logger.info(new Date().toLocaleString()+",�?��发�?消息["+atomicLong.longValue()+"]�?);
//						System.out.println(new Date().toLocaleString()+",APPEvent send message["+atomicLong1.longValue()+"] time");
						System.out.println(new Date().toLocaleString()+","+Thread.currentThread().getName() + " -event:" + ++count);
						/*try {
							Thread.sleep(50);
						} catch (InterruptedException e) {
							logger.error(key+"休眠异常",e);
						}*/
					}
				}
				
			}.start();
		}
	

	}
}
