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
public class H5KafkaProducerTest2 {
	private static Logger logger = LoggerFactory.getLogger(H5KafkaProducerTest2.class);
	
	static AtomicLong atomicLong = new AtomicLong();
	static AtomicLong atomicLong1 = new AtomicLong();
	static final String topic_pv = "topic_webpv";
	static final String topic_event = "topic_appevent";
	
	
	/**构�?H5的PV测试数据
	 * @param pageId
	 * @return
	 */
	private static String buildH5PvData(){
		StringBuilder builder = new StringBuilder();
		String[] cookie=new String[]{"0x1cf5bb525f49d9f5","0x17916d8805b59d4e","0x17916d8805b59d4r","0x17916d8805b59d4q","0x17916d8805b59d4w"};
		builder.append(cookie[new Random().nextInt(cookie.length)]);
//		builder.append(cookie[1]);
		builder.append(HbConstants.FIELD_SEPARATOR);
		builder.append(System.currentTimeMillis());
		builder.append(HbConstants.FIELD_SEPARATOR);
		builder.append("1");
		builder.append(HbConstants.FIELD_SEPARATOR);
		builder.append("1");
		builder.append(HbConstants.FIELD_SEPARATOR);
		builder.append("1");
		builder.append(HbConstants.FIELD_SEPARATOR);
		builder.append("1");
		builder.append(HbConstants.FIELD_SEPARATOR);
		builder.append("1");
		builder.append(HbConstants.FIELD_SEPARATOR);
		builder.append("1");
		builder.append(HbConstants.FIELD_SEPARATOR);
		builder.append("1");
		builder.append(HbConstants.FIELD_SEPARATOR);
		builder.append("1");
		builder.append(HbConstants.FIELD_SEPARATOR);
		String[] htag =new String[]{"0.0130010001900000","0.0130020001900000","0.0130020002900000","0.0130010002900000"};
		builder.append(htag[new Random().nextInt(htag.length)]);
		builder.append(HbConstants.FIELD_SEPARATOR);
		builder.append("1");
		builder.append(HbConstants.FIELD_SEPARATOR);
		builder.append("https://trade.ehowbuy.com/acs/active/2016010701/index.html?HTAG=0.0120020002900000");
		builder.append(HbConstants.FIELD_SEPARATOR);
		builder.append("1");
		builder.append(HbConstants.FIELD_SEPARATOR);
		builder.append("1");
		builder.append(HbConstants.FIELD_SEPARATOR);
		builder.append("1");
		builder.append(HbConstants.FIELD_SEPARATOR);
		builder.append("1");
		builder.append(HbConstants.FIELD_SEPARATOR);
		builder.append("1");
		builder.append(HbConstants.FIELD_SEPARATOR);
		String[] pageid = new String[]{"2016012216","201621000","201621010","2016012226","2016012236"};
		builder.append(pageid[new Random().nextInt(pageid.length)]);
		builder.append(HbConstants.FIELD_SEPARATOR);
		String[] level=new String[]{"2","3"};
		builder.append(level[new Random().nextInt(level.length)]);
		builder.append(HbConstants.FIELD_SEPARATOR);
		builder.append("1");
		builder.append(HbConstants.FIELD_SEPARATOR);
		builder.append("1");
	    builder.append(HbConstants.FIELD_SEPARATOR);
		builder.append("5002");
		builder.append(HbConstants.FIELD_SEPARATOR);
		builder.append("1");
		builder.append(HbConstants.FIELD_SEPARATOR);
		builder.append("1");
		builder.append(HbConstants.FIELD_SEPARATOR);
		builder.append("1");
		builder.append(HbConstants.FIELD_SEPARATOR);
		builder.append("1");
		return builder.toString();
	}
	
	
	/**
	 * 构�?�?��数据
	 * @param pageId
	 * @return
	 */
	private static String buildH5Event(){
		StringBuilder builder = new StringBuilder();

		builder.append(System.currentTimeMillis());
		builder.append(HbConstants.FIELD_SEPARATOR);
//		String[] type = new String[]{"4","5","6","8","9","10","13","14"};
		String[] type = new String[]{"4"};
//		String[] type = new String[]{"4"};
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
		String[] proid = new String[]{"5002"};
		builder.append(proid[new Random().nextInt(proid.length)]);
		builder.append(HbConstants.FIELD_SEPARATOR);
		String[] otag =new String[]{"0120010001900000-0-0-0.0","0120020002900000-0-0-0.0","0120010003900000-0-0-0.0",
				"0130010001900000-0-0-0.0","0130010002900000-0-0-0.0","0130020001900000-0-0-0.0"};
		builder.append(otag[new Random().nextInt(otag.length)]);// 访问路径跟踪 otag
//		builder.append(otag[3]);// 访问路径跟踪 otag
		return builder.toString();
	}
	
	
	public static void main(String[] args) {
		KafkaProducer producer = new KafkaProducer(topic_pv);
		for(int i=0;i<0000;i++){
			String message =buildH5PvData();
			String key = Thread.currentThread().getName();
			producer.pubMessage(key+Math.random(), message);
			System.out.println(message);
		}
		 
		
//		KafkaProducer producer = new KafkaProducer(topic_event);
//		for(int i =0; i <7000; i++){
//			String message =buildH5Event();
//			String key = Thread.currentThread().getName();
//			producer.pubMessage(key+Math.random(), message);
//			System.out.println(message);
//			
//		}
	

	}
}
