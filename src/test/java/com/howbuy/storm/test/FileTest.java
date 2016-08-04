/**
 * 
 */
package com.howbuy.storm.test;

import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.howbuy.storm.common.HbConstants;
import com.howbuy.storm.dto.ChannelEventAccount;
import com.howbuy.storm.enums.AppProidType;

/**
 * @author qiankun.li
 *
 */
public class FileTest {
	private Logger logger = LoggerFactory.getLogger(getClass());
	Set<String> tradeTypeSet = new HashSet<String>(){
		/**
		 * 
		 */
		private static final long serialVersionUID = -4996724465506139871L;

		{
			add("6");//买基�?
			add("8");//储蓄罐存�?
			add("9");//储蓄罐取�?
			add("10");//定投
			add("13");//储蓄罐活期转定期
			add("14");//储蓄罐直接购买定�?
		}
	};
	/**
	 * 3001	掌上基金iPhone APP�?002 �?掌上基金安卓APP
	 */
	Set<String> zjCodeSet = new HashSet<String>(){
		/**
		 * 
		 */
		private static final long serialVersionUID = -4996724465506139871L;

		{
			add(AppProidType.APP_PROID_ZJ_IPHONE.getIndex());
			add(AppProidType.APP_PROID_ZJ_ANDROID.getIndex());
		}
	};
	/**
	 * 2001	储蓄罐iPhone APP�?002 �?储蓄罐安卓APP
	 */
	Set<String> cxgCodeSet = new HashSet<String>(){
		/**
		 * 
		 */
		private static final long serialVersionUID = -5308998214124278831L;

		{
			add(AppProidType.APP_PROID_CXG_IPHONE.getIndex());
			add(AppProidType.APP_PROID_CXG_ANDROID.getIndex());
		}
	};
	private Timestamp getCt(Calendar now) {
		now.set(Calendar.SECOND, 0);
		now.set(Calendar.MILLISECOND, 0);
		DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		java.util.Date date = now.getTime();
		String mystrdate = df.format(date);
		return java.sql.Timestamp.valueOf(mystrdate);
	}
	
	
	private void  writeDataToDB(String path) throws IOException{
		Calendar now = Calendar.getInstance();
		Timestamp ct = getCt(now);
		final List<ChannelEventAccount> objectList = new ArrayList<ChannelEventAccount>();
		String threadName =  Thread.currentThread().getName();
		//访问次数（激活次数）//不去�?
		Map<String,Integer> activationMap = new HashMap<String,Integer>(0);
		//�?��人数 去重
		Map<String,Set<String>> openAccMap = new HashMap<String,Set<String>>(0);
		//绑卡人数 去重
		Map<String,Set<String>> bindCardMap = new HashMap<String,Set<String>>(0);
		//下单人数 去重
		Map<String,Set<String>> orderMap = new HashMap<String,Set<String>>(0);
		//下单金额 累加
		Map<String,BigDecimal> orderAmountMap = new HashMap<String,BigDecimal>(0);
		//平台号�?网点号数�?
		Map<String,String> poMap = new HashMap<String,String>(0);
		
		final String regex = "&";

		List<String> files = FileUtils.readLines(new File(path), "UTF-8");
		for(String str:files){
			logger.debug(threadName + ",poll data:"+ str);
			String[] values = str.split(HbConstants.FIELD_SEPARATOR);
			if(values!=null){
				String key = null;
				if(values.length==15){//�?��数据
					logger.debug(threadName + ",poll Activation data:"+ str);
					String time = values[0];
					Calendar c = Calendar.getInstance();
					c.setTimeInMillis(Long.valueOf(time));
					System.out.println(c.getTime().toLocaleString());
					String proid = values[1];
					String outletcode = values[4];
					key = proid + regex + outletcode;
					Integer acNum = activationMap.get(key);
					if(null == acNum)
						acNum = new Integer(0);
					acNum++;
					activationMap.put(key, acNum);
				}else if(values.length==23){//APPevent数据
					String time = values[0];
					Calendar c = Calendar.getInstance();
					c.setTimeInMillis(Long.valueOf(time));
					System.out.println(c.getTime().toLocaleString());
					logger.debug(threadName + ",poll APPevent data:"+ str);
					String type=values[1];
					String amount = values[2];
					String proid = values[19];
					String guid = values[15];
					// 匹配数据
					String outletcode = values[13];
					key = proid + regex + outletcode;
					//4-�?���?-绑卡  ,下单�?�?�?�?0�?3�?4
					if("4".equals(type)){//�?��
						Set<String> openNumSet = openAccMap.get(key);
						if(null == openNumSet)
							openNumSet = new HashSet<String>();
						
						openNumSet.add(guid);
						openAccMap.put(key, openNumSet);
					}else if("5".equals(type)){//绑卡
						Set<String> bindNumSet = bindCardMap.get(key);
						if(null == bindNumSet)
							bindNumSet = new  HashSet<String>();
						
						bindNumSet.add(guid);
						bindCardMap.put(key, bindNumSet);
					}else if(tradeTypeSet.contains(type)){
						Set<String> orderNumSet = orderMap.get(key);
						if(null == orderNumSet)//下单人数
							orderNumSet = new HashSet<String>(0);
						
						orderNumSet.add(guid);
						orderMap.put(key, orderNumSet);
						
						//下单金额
						BigDecimal amountSum=orderAmountMap.get(key);
						if(null == amountSum)
							amountSum = new BigDecimal(0);
						
						try {
							amountSum = amountSum.add(new BigDecimal(amount));
						} catch (NumberFormatException e) {
							logger.error(amount+" cast to float error",e);
						}
						orderAmountMap.put(key, amountSum);
					}
				}
				if(StringUtils.isNotBlank(key))
					poMap.put(key, key);
			} 
		}
		
	
		Set<String> sets = poMap.keySet();
		for(String po:sets){
			String[] pos = po.split(regex);
			String proid = pos[0];
			String outletcode = pos[1];
			Integer activateNum = activationMap.get(po)==null?0:activationMap.get(po);
			Integer openNum = openAccMap.get(po)==null?0:openAccMap.get(po).size();
			Integer bindNum = bindCardMap.get(po)==null?0:bindCardMap.get(po).size();
			Integer orderNum = orderMap.get(po)==null?0:orderMap.get(po).size();
			BigDecimal orderAmount = orderAmountMap.get(po)==null?new BigDecimal(0):orderAmountMap.get(po);
			ChannelEventAccount eventAccount = new ChannelEventAccount();
			eventAccount.setActivateNum(activateNum);
			eventAccount.setBindcardNum(bindNum);
			eventAccount.setCreateTime(ct);
			eventAccount.setOpenaccNum(openNum);
			eventAccount.setOrderNum(orderNum);
			eventAccount.setOutletcode(outletcode);
			eventAccount.setProid(proid);
			eventAccount.setOrderAmount(orderAmount);
			objectList.add(eventAccount);
		}
		System.out.println("proid\tActivateNum\tBindcardNum\tOpenaccNum\tOrderNum\tOrderAmount\ttime");
		for(ChannelEventAccount c:objectList){
			System.out.println(c.getProid()+"\t"+c.getActivateNum()+"\t"+c.getBindcardNum()+"\t"+c.getOpenaccNum()+"\t"+c.getOrderNum()+"\t"+c.getOrderAmount()+"\t"+c.getCreateTime());
		}
		logger.info("----end insert objectList DB----");
	
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		FileTest fileTest = new FileTest();
		try {
			fileTest.writeDataToDB("E://000000_0");
		} catch (IOException e) {
			e.printStackTrace();
		}
		try {
			fileTest.writeDataToDB("E://000001_0");
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
