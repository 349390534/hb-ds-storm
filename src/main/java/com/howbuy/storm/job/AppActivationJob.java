/**
 * 
 */
package com.howbuy.storm.job;

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
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.howbuy.storm.common.HbConstants;
import com.howbuy.storm.dao.ChannelEventAccountDao;
import com.howbuy.storm.dto.ChannelEventAccount;

/**
 * @author qiankun.li
 * APPevent事件流量处理job -单例
 */
public class AppActivationJob {

	private Logger logger = LoggerFactory.getLogger(getClass());
	
	private static AppActivationJob job = new AppActivationJob();

	private volatile LinkedBlockingQueue<String> appActivationQueue = new LinkedBlockingQueue<String>();
	
	private final ReentrantReadWriteLock rwl = new ReentrantReadWriteLock();

	//入库开关
	private volatile boolean isInDB = false;
	
	private ChannelEventAccountDao eventAccountDao = new ChannelEventAccountDao();
	
	private AppActivationJob() {
	}
	
	public static AppActivationJob getJob(){
		return job;
	}
	
	public void putIntoQueue(String data) {
		rwl.writeLock().lock();// 取到写锁
		try {
			// 加入队列，5s超时
			boolean o = appActivationQueue.offer(data, 5, TimeUnit.SECONDS);
			if (!o) {
				logger.warn("appActivationQueue is full ");
			} else {
				logger.debug("insert ["+data+"] ok");
			}
		} catch (InterruptedException e) {
			logger.error("插入数据异常", e);
		} finally {
			rwl.writeLock().unlock();// 释放写锁
		}

	}
	
	public synchronized void excute(){
		new Thread(){
			@Override
			public void run() {
				logger.info("AppActivationJob start......");
				long millis = HbConstants.SLEEPTIME_MIN *60 *1000;
				
				//判断首次启动时间,头一个五分钟的时刻
				while(true){
					Calendar c = Calendar.getInstance();
					int MINUTE = c.get(Calendar.MINUTE);
					int SECOND = c.get(Calendar.SECOND);
					int MILLISECOND = c.get(Calendar.MILLISECOND);
					if((MINUTE +1) % 5==0 && SECOND == 59 && MILLISECOND > 1 ){//时间误差保证小于1秒之内
						final Calendar cn = c;
						//当前时间往后推移1秒
						cn.set(Calendar.SECOND, cn.get(Calendar.SECOND)+1);
						cn.set(Calendar.MILLISECOND, cn.get(Calendar.MILLISECOND)+1);
						rwl.readLock().lock();
						try {
							writeDataToDB(cn);
						} catch (Exception e) {
							logger.error("insert db Exception", e);
							isInDB=false;
						} finally {
							rwl.readLock().unlock();
							//休眠4分钟，等待下一个五分钟之前的一分钟启动线程
							try {
								Thread.sleep(millis);
							} catch (InterruptedException e) {
								logger.error(Thread.currentThread().getName() 
										+"sleep Exception", e);
							}
						}
						
					}
				}
			}
		}.start();
		
	}
	
	
	Set<String> tradeTypeSet = new HashSet<String>(){
		/**
		 * 
		 */
		private static final long serialVersionUID = -4996724465506139871L;

		{
			add("6");//买基金
			add("8");//储蓄罐存入
			add("9");//储蓄罐取现
			add("10");//定投
			add("13");//储蓄罐活期转定期
			add("14");//储蓄罐直接购买定期
		}
	};
	
	private synchronized void writeDataToDB(final Calendar now){
		if(isInDB){
			logger.debug("already insert DB");
			return;
		}
		if(appActivationQueue.isEmpty()){
			logger.debug("----no data need to insert DB----");
			return;
		}
		isInDB = true;
		LinkedBlockingQueue<String> tempBlockingQueue = new LinkedBlockingQueue<String>();  
		// 将blockingQueue的数据引用赋值给tempBlockingQueue
		tempBlockingQueue = appActivationQueue;
		// 将blockingQueue重新new出一个对象。
		appActivationQueue = new LinkedBlockingQueue<String>();
		
		logger.info("----start insert DB----");
		logger.info("all data:"+tempBlockingQueue.size());
		//Date dt = new Date(now.getTimeInMillis());
		Timestamp ct = getCt(now);
		final List<ChannelEventAccount> objectList = new ArrayList<ChannelEventAccount>();
		String threadName =  Thread.currentThread().getName();
		//访问次数（激活次数）//不去重
		Map<String,Integer> activationMap = new HashMap<String,Integer>(0);
		//开户人数 去重
		Map<String,Set<String>> openAccMap = new HashMap<String,Set<String>>(0);
		//绑卡人数 去重
		Map<String,Set<String>> bindCardMap = new HashMap<String,Set<String>>(0);
		//下单人数 去重
		Map<String,Set<String>> orderMap = new HashMap<String,Set<String>>(0);
		//下单金额 累加
		Map<String,BigDecimal> orderAmountMap = new HashMap<String,BigDecimal>(0);
		//平台号、网点号数据
		Map<String,String> poMap = new HashMap<String,String>(0);
		
		final String regex = "&";
		while (!tempBlockingQueue.isEmpty()) {
			// 取出数据处理
			String str = tempBlockingQueue.poll();
			if(null != str){
				logger.debug(threadName + ",poll data:"+ str);
				String[] values = str.split(HbConstants.FIELD_SEPARATOR);
				if(values!=null){
					String key = null;
					if(values.length==13){//激活数据
						logger.debug(threadName + ",poll Activation data:"+ str);
						String proid = values[1];
						String outletcode = values[4];
						key = proid + regex + outletcode;
						Integer acNum = activationMap.get(key);
						if(null == acNum)
							acNum = new Integer(0);
						acNum++;
						activationMap.put(key, acNum);
					}else if(values.length==21){//APPevent数据
						logger.debug(threadName + ",poll APPevent data:"+ str);
						String type=values[1];
						String amount = values[2];
						String proid = values[19];
						String guid = values[15];
						// 匹配数据
						String outletcode = values[13];
						key = proid + regex + outletcode;
						//4-开户、5-绑卡  ,下单：6、8、9、10、13、14
						if("4".equals(type)){//开户
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
		
		new Thread(){
			@Override
			public void run() {
				eventAccountDao.insertList(objectList);
				logger.info("----end insert objectList DB----");
			}
			
		}.start();
		isInDB = false;
	}
	
	private Timestamp getCt(Calendar now) {
		now.set(Calendar.SECOND, 0);
		now.set(Calendar.MILLISECOND, 0);
		DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		java.util.Date date = now.getTime();
		String mystrdate = df.format(date);
		return java.sql.Timestamp.valueOf(mystrdate);
	}
	
	
}
