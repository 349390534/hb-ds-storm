/**
 * 
 */
package com.howbuy.storm.job;

import java.sql.Date;
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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.howbuy.storm.common.HbConstants;
import com.howbuy.storm.dao.ChannelPagePvUvDao;
import com.howbuy.storm.dto.ChannelPageRecord;
import com.howbuy.storm.enums.ProidType;

/**
 * @author qiankun.li
 * 资讯流量处理job -单例
 */
public class WebNewsPvJob {

	private Logger logger = LoggerFactory.getLogger(getClass());
	
	private static WebNewsPvJob job = new WebNewsPvJob();

	private LinkedBlockingQueue<String> webNewsQueue = new LinkedBlockingQueue<String>();
	
	//private LinkedBlockingQueue<String> tempQueue = new LinkedBlockingQueue<String>();

	private final ReentrantReadWriteLock rwl = new ReentrantReadWriteLock();

	//private Map<String,Object> reduceMap = Collections.synchronizedMap(new HashMap<String,Object>(0)); 
	
	private ChannelPagePvUvDao pagePvUvDao = new ChannelPagePvUvDao();
	/**
	 *启动线程开关 
	 */
	private boolean isStart = false;
	
	//入库开关
	//private volatile boolean isInDB = false;
	
	private WebNewsPvJob() {
	}
	
	public static WebNewsPvJob getJob(){
		
		return job;
	}
	
	public void putIntoQueue(String data) {
		rwl.writeLock().lock();// 取到写锁
		try {
			// 加入队列，5s超时
			boolean o = webNewsQueue.offer(data, 5, TimeUnit.SECONDS);
			if (!o) {
				logger.warn("webNewsQueue is full ");
			} else {
				logger.info("insert ok");
			}
		} catch (InterruptedException e) {
			logger.error("插入数据异常", e);
		} finally {
			rwl.writeLock().unlock();// 释放写锁
		}

	}
	
	public void excute(){
		new Thread(){
			{
				//开关打开
				isStart = true;
			}
			@Override
			public void run() {
				logger.info("WebNewsPvJob start......");
				long millis = HbConstants.SLEEPTIME_MIN *60 *1000;
				while(true){
					try {
						Thread.sleep(millis);
						//Thread.sleep(5000);
					} catch (InterruptedException e) {
						logger.error(Thread.currentThread().toString() + "sleep error",e);
					}
					rwl.readLock().lock();
					try {
						writeDataToDB();
						
						pageIdSet.clear();
					} catch (Exception e) {
						logger.error("读取数据线程异常",e);
					}finally{
						 rwl.readLock().unlock();
					}
					
				}
			}
		}.start();
		
	}
	
	private ProidType proidType = ProidType.PROID_ZIXUN;//定义模块 资讯模块
	
	Set<String> pageIdSet = new HashSet<String>();
	
	private void writeDataToDB(){
		//启动一个线程去处理数据，保证不影响下一次数据的读取操作  ,达到异步入库
		new Thread(){
			private LinkedBlockingQueue<String> tempBlockingQueue = new LinkedBlockingQueue<String>();  
			{
				// 将blockingQueue的数据引用赋值给tempBlockingQueue
				tempBlockingQueue = webNewsQueue;
				// 将blockingQueue重新new出一个对象。
				webNewsQueue = new LinkedBlockingQueue<String>();
			}
			
			@Override
			public void run() {

				try {

					logger.info("----处理数据开始----");
					// isInDB = true;
					List<ChannelPageRecord> objectList = new ArrayList<ChannelPageRecord>();
					Map<String, Integer> pMap = new HashMap<String, Integer>();
					Map<String, Set<String>> uMap = new HashMap<String, Set<String>>();

					Calendar now = Calendar.getInstance();
					Date dt = new Date(now.getTimeInMillis());
					Timestamp ct = getCt(now);
					while (!tempBlockingQueue.isEmpty()) {
						// 取出数据处理
						String str = tempBlockingQueue.poll();
						
						if(null != str){
							
							logger.info(Thread.currentThread().getName() + ",消费数据:"
									+ str);
							String[] values = str
									.split(HbConstants.FIELD_SEPARATOR);
							String guid = values[0];
							String pageid = values[18];
							pageIdSet.add(pageid);
							Integer p = pMap.get(pageid);
							if (null == p)
								p = 0;
							pMap.put(pageid, ++p);
							
							Set<String> us = uMap.get(pageid);
							if (us == null)
								us = new HashSet<String>();
							us.add(guid);
							uMap.put(pageid, us);
						}
					}

					for (String pageId : pageIdSet) {
						ChannelPageRecord record = new ChannelPageRecord();
						record.setCreatetime(ct);
						record.setDt(dt);
						record.setPageId(pageId);
						record.setProid(proidType.getIndex());
						long pv = pMap.get(pageId);
						record.setPv(pv);
						long uv = uMap.get(pageId).size();
						record.setUv(uv);
						objectList.add(record);
					}

					// 入库操作 //isInDB = false;
					pagePvUvDao.insertList(objectList);
					
					logger.info("----数据入库结束----");

				} catch (Exception e) {
					logger.error("处理数据异常",e);
				}
			}
		}.start();
	}
	
	
	private Timestamp getCt(Calendar now) {
		now.set(Calendar.SECOND, 0);
		now.set(Calendar.MILLISECOND, 0);
		DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		java.util.Date date = now.getTime();
		String mystrdate = df.format(date);
		return java.sql.Timestamp.valueOf(mystrdate);
	}
	
	
	public boolean isStart() {
		return isStart;
	}

	/*{
		if(!isStart){
			excute();
		}
	}
*/
	
	
}
