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
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.howbuy.storm.common.HbConstants;
import com.howbuy.storm.dao.ChannelEventAccountDao;
import com.howbuy.storm.dto.ChannelEventAccount;
import com.howbuy.storm.enums.AppProidType;

/**
 * @author qiankun.li
 * APPevent事件流量处理job -单例
 */
public class AppActivationAllJob {

	private Logger logger = LoggerFactory.getLogger(getClass());
	
	private static AppActivationAllJob job = new AppActivationAllJob();

	private volatile LinkedBlockingQueue<String> appActivationQueue = new LinkedBlockingQueue<String>();
	
	private final ReentrantReadWriteLock rwl = new ReentrantReadWriteLock();
	
	//入库开关
	private volatile boolean isInDB = false;
	
	private ChannelEventAccountDao eventAccountDao = new ChannelEventAccountDao();
	
	private AppActivationAllJob() {
	}
	
	public static AppActivationAllJob getJob(){
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
				logger.info("AppActivationAllJob start......");
				long millis = HbConstants.SLEEPTIME_MIN *60 *1000;
				
				//判断首次启动时间,头一个五分钟的时刻
				while(true){
					Calendar c = Calendar.getInstance();
					int MINUTE = c.get(Calendar.MINUTE);
					int SECOND = c.get(Calendar.SECOND);
					int MILLISECOND = c.get(Calendar.MILLISECOND);
					if((MINUTE +1) % 5==0 && SECOND == 59 && MILLISECOND > 1){
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
	/**
	 * 3001	掌上基金iPhone APP、3002 – 掌上基金安卓APP
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
	 * 2001	储蓄罐iPhone APP、2002 – 储蓄罐安卓APP
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
		String threadName =  Thread.currentThread().getName();
		//访问次数(激活次数)
		Long activationZjDisSet_Iphone = new Long(0);//掌基iPhone激活人数(去重)
		Long activationZjDisSet_Android = new Long(0);//掌基安卓激活人数(去重)
		Long activationCxgDisSet_Iphone = new Long(0);
		Long activationCxgDisSet_Android = new Long(0);
		
		//开户人数
		Set<String> openAccZjDisSet_Iphone = new HashSet<String>(0);
		Set<String> openAccZjDisSet_Android = new HashSet<String>(0);
		Set<String> openAccCxgDisSet_Iphone = new HashSet<String>(0);
		Set<String> openAccCxgDisSet_Android = new HashSet<String>(0);
		//绑卡人数
		Set<String> bindCardZjDisSet_Iphone = new HashSet<String>(0);
		Set<String> bindCardZjDisSet_Android = new HashSet<String>(0);
		Set<String> bindCardCxgDisSet_Iphone = new HashSet<String>(0);
		Set<String> bindCardCxgDisSet_Android = new HashSet<String>(0);
		
		//下单人数
		Set<String> orderZjDisSet_Iphone = new HashSet<String>(0);
		Set<String> orderZjDisSet_Android = new HashSet<String>(0);
		Set<String> orderCxgDisSet_Iphone = new HashSet<String>(0);
		Set<String> orderCxgDisSet_Android = new HashSet<String>(0);
		
		//下单金额
		BigDecimal orderAmountZj_Iphone = new BigDecimal(0);
		BigDecimal orderAmountZj_Android = new BigDecimal(0);
		BigDecimal orderAmountCxg_Iphone = new BigDecimal(0);
		BigDecimal orderAmountCxg_Android = new BigDecimal(0);
		
		
		
		Set<String> proidSet=new HashSet<String>();
		while (!tempBlockingQueue.isEmpty()) {
			// 取出数据处理
			String str = tempBlockingQueue.poll();
			if(null != str){
				logger.debug(threadName + ",poll data:"+ str);
				String[] values = str.split(HbConstants.FIELD_SEPARATOR);
				if(values!=null){
					String proid = null;
					if(values.length==13){//激活数据
						logger.debug(threadName + ",poll Activation data:"+ str);
						proid = values[1];
						//String guid = values[3];
						//判断掌基，区分安卓和IOS
						if(zjCodeSet.contains(proid)){
							if(AppProidType.APP_PROID_ZJ_IPHONE.getIndex().equals(proid)){//iPhone
								activationZjDisSet_Iphone++;
							}else if(AppProidType.APP_PROID_ZJ_ANDROID.getIndex().equals(proid)){//安卓
								activationZjDisSet_Android++;
							}
						}
						//判断储蓄罐,区分安卓和IOS
						if(cxgCodeSet.contains(proid)){
							if(AppProidType.APP_PROID_CXG_IPHONE.getIndex().equals(proid)){//iPhone
								activationCxgDisSet_Iphone++;
							}else if(AppProidType.APP_PROID_CXG_ANDROID.getIndex().equals(proid)){//Android
								activationCxgDisSet_Android++;
							}
						}
						
					}else if(values.length==21){//APPevent数据
						logger.debug(threadName + ",poll APPevent data:"+ str);
						String type=values[1];
						String amount = values[2];
						proid = values[19];
						String guid = values[15];
						//4-开户、5-绑卡  ,下单：6、8、9、10、13、14
						if("4".equals(type)){
							//判断掌基，区分安卓和IOS
							if(zjCodeSet.contains(proid)){
								if(AppProidType.APP_PROID_ZJ_IPHONE.getIndex().equals(proid)){//iPhone
									openAccZjDisSet_Iphone.add(guid);
								}else if(AppProidType.APP_PROID_ZJ_ANDROID.getIndex().equals(proid)){//安卓
									openAccZjDisSet_Android.add(guid);
								}
							}
							//判断储蓄罐,区分安卓和IOS
							if(cxgCodeSet.contains(proid)){
								if(AppProidType.APP_PROID_CXG_IPHONE.getIndex().equals(proid)){//iPhone
									openAccCxgDisSet_Iphone.add(guid);
								}else if(AppProidType.APP_PROID_CXG_ANDROID.getIndex().equals(proid)){//Android
									openAccCxgDisSet_Android.add(guid);
								}
							}
						}else if("5".equals(type)){
							//判断掌基，区分安卓和IOS
							if(zjCodeSet.contains(proid)){
								if(AppProidType.APP_PROID_ZJ_IPHONE.getIndex().equals(proid)){//iPhone
									bindCardZjDisSet_Iphone.add(guid);
								}else if(AppProidType.APP_PROID_ZJ_ANDROID.getIndex().equals(proid)){//安卓
									bindCardZjDisSet_Android.add(guid);
								}
							}
							//判断储蓄罐,区分安卓和IOS
							if(cxgCodeSet.contains(proid)){
								if(AppProidType.APP_PROID_CXG_IPHONE.getIndex().equals(proid)){//iPhone
									bindCardCxgDisSet_Iphone.add(guid);
								}else if(AppProidType.APP_PROID_CXG_ANDROID.getIndex().equals(proid)){//Android
									bindCardCxgDisSet_Android.add(guid);
								}
							}
						}else if(tradeTypeSet.contains(type)){
							try {
								//判断掌基，区分安卓和IOS
								if(zjCodeSet.contains(proid)){
									if(AppProidType.APP_PROID_ZJ_IPHONE.getIndex().equals(proid)){//iPhone
										orderZjDisSet_Iphone.add(guid);
										orderAmountZj_Iphone = orderAmountZj_Iphone.add(new BigDecimal(amount));
									}else if(AppProidType.APP_PROID_ZJ_ANDROID.getIndex().equals(proid)){//安卓
										orderZjDisSet_Android.add(guid);
										orderAmountZj_Android = orderAmountZj_Android.add(new BigDecimal(amount));
									}
								}
								//判断储蓄罐,区分安卓和IOS
								if(cxgCodeSet.contains(proid)){
									if(AppProidType.APP_PROID_CXG_IPHONE.getIndex().equals(proid)){//iPhone
										orderCxgDisSet_Iphone.add(guid);
										orderAmountCxg_Iphone = orderAmountCxg_Iphone.add(new BigDecimal(amount));
									}else if(AppProidType.APP_PROID_CXG_ANDROID.getIndex().equals(proid)){//Android
										orderCxgDisSet_Android.add(guid);
										orderAmountCxg_Android = orderAmountCxg_Android.add(new BigDecimal(amount));
									}
								}
							} catch (NumberFormatException e) {
								logger.error(amount+" cast to float error",e);
							}
						}
					}
					proidSet.add(proid);
				} 
			}
		}
		//汇总数据
		final List<ChannelEventAccount> allList = new ArrayList<ChannelEventAccount>();
		for(String proid :proidSet){
			if(AppProidType.APP_PROID_CXG_ANDROID.getIndex().equals(proid)){
				ChannelEventAccount cxgAndroid = buildCxgAndroid(activationCxgDisSet_Android, openAccCxgDisSet_Android, bindCardCxgDisSet_Android, orderCxgDisSet_Android,orderAmountCxg_Android, ct);
				allList.add(cxgAndroid);
			}else if(AppProidType.APP_PROID_CXG_IPHONE.getIndex().equals(proid)){
				ChannelEventAccount cxgIphone = buildCxgIphone(activationCxgDisSet_Iphone, openAccCxgDisSet_Iphone, bindCardCxgDisSet_Iphone, orderCxgDisSet_Iphone,orderAmountCxg_Iphone, ct);
				allList.add(cxgIphone);
			}else if(AppProidType.APP_PROID_ZJ_IPHONE.getIndex().equals(proid)){
				ChannelEventAccount zjIphone= buildZjIphone(activationZjDisSet_Iphone, openAccZjDisSet_Iphone, bindCardZjDisSet_Iphone, orderZjDisSet_Iphone,orderAmountZj_Iphone,ct);
				allList.add(zjIphone);
			}else if(AppProidType.APP_PROID_ZJ_ANDROID.getIndex().equals(proid)){
				ChannelEventAccount zjAndroid=buildZjAndroid(activationZjDisSet_Android, openAccZjDisSet_Android, bindCardZjDisSet_Android, orderZjDisSet_Android,orderAmountZj_Android, ct);
				allList.add(zjAndroid);
			}
		}
		new Thread(){
			@Override
			public void run() {
				eventAccountDao.insertList(allList);
				logger.info("----end insert allList DB----");
			}
		}.start();
		isInDB = false;
	}
	
	/**掌基iPhone汇总数据
	 * @param activationZjDisSet_Iphone
	 * @param openAccZjDisSet_Iphone
	 * @param bindCardZjDisSet_Iphone
	 * @param orderZjDisSet_Iphone
	 * @return
	 */
	private ChannelEventAccount buildZjIphone(Long activationZjDisSet_Iphone,
			Set<String>	openAccZjDisSet_Iphone,Set<String> bindCardZjDisSet_Iphone,
			Set<String> orderZjDisSet_Iphone,BigDecimal orderAmountZj_Iphone,Timestamp createTime) {
		ChannelEventAccount account = new ChannelEventAccount();
		account.setActivateNum(activationZjDisSet_Iphone.intValue());
		account.setOpenaccNum(openAccZjDisSet_Iphone.size());
		account.setBindcardNum(bindCardZjDisSet_Iphone.size());
		account.setCreateTime(createTime);
		account.setOrderNum(orderZjDisSet_Iphone.size());
		account.setOrderAmount(orderAmountZj_Iphone);
		account.setOutletcode("-1");
		account.setProid(AppProidType.APP_PROID_ZJ_IPHONE.getIndex());
		return account;
	}

	private ChannelEventAccount buildZjAndroid(Long activationZjDisSet_Android,
			Set<String>	openAccZjDisSet_Android,Set<String> bindCardZjDisSet_Android,
			Set<String> orderZjDisSet_Android,BigDecimal orderAmountZj_Android,Timestamp createTime) {
		ChannelEventAccount account = new ChannelEventAccount();
		account.setActivateNum(activationZjDisSet_Android.intValue());
		account.setOpenaccNum(openAccZjDisSet_Android.size());
		account.setBindcardNum(bindCardZjDisSet_Android.size());
		account.setCreateTime(createTime);
		account.setOrderAmount(orderAmountZj_Android);
		account.setOrderNum(orderZjDisSet_Android.size());
		account.setOutletcode("-1");
		account.setProid(AppProidType.APP_PROID_ZJ_ANDROID.getIndex());
		return account;
	}

	private ChannelEventAccount buildCxgIphone(Long activationCxgDisSet_Iphone,
			Set<String>	openAccCxgDisSet_Iphone,Set<String> bindCardCxgDisSet_Iphone,
			Set<String> orderCxgDisSet_Iphone,BigDecimal orderAmountCxg_Iphone,Timestamp createTime) {
		ChannelEventAccount account = new ChannelEventAccount();
		account.setActivateNum(activationCxgDisSet_Iphone.intValue());
		account.setOpenaccNum(openAccCxgDisSet_Iphone.size());
		account.setBindcardNum(bindCardCxgDisSet_Iphone.size());
		account.setCreateTime(createTime);
		account.setOrderAmount(orderAmountCxg_Iphone);
		account.setOrderNum(orderCxgDisSet_Iphone.size());
		account.setOutletcode("-1");
		account.setProid(AppProidType.APP_PROID_CXG_IPHONE.getIndex());
		return account;
	}

	private ChannelEventAccount buildCxgAndroid(Long activationCxgDisSet_Android,
			Set<String>	openAccCxgDisSet_Android,Set<String> bindCardCxgDisSet_Android,
			Set<String> orderCxgDisSet_Android,BigDecimal orderAmountCxg_Android,Timestamp createTime) {
		ChannelEventAccount account = new ChannelEventAccount();
		account.setActivateNum(activationCxgDisSet_Android.intValue());
		account.setOpenaccNum(openAccCxgDisSet_Android.size());
		account.setBindcardNum(bindCardCxgDisSet_Android.size());
		account.setCreateTime(createTime);
		account.setOrderAmount(orderAmountCxg_Android);
		account.setOrderNum(orderCxgDisSet_Android.size());
		account.setOutletcode("-1");
		account.setProid(AppProidType.APP_PROID_CXG_ANDROID.getIndex());
		return account;
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
