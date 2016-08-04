package com.howbuy.storm.h5;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

import com.howbuy.storm.common.HbConstants;
import com.howbuy.storm.dao.ChannelEventAccountDao;

public class H5GatherBolt extends BaseRichBolt {
	
	private Logger logger = LoggerFactory.getLogger(H5GatherBolt.class);
	
	private OutputCollector collector;

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	private Map<String,List<Object[]>> eventMap = new HashMap<String,List<Object[]>>();
	
	private Map<String,List<Object[]>> pvMap = new HashMap<String,List<Object[]>>();
	
	private static ChannelEventAccountDao eventAccountDao = new ChannelEventAccountDao();
	
	private static final int ARRAY_LEN = 13;

	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		
		this.collector = collector;
		
		new Thread(){

			@SuppressWarnings("unchecked")
			@Override
			public void run() {
				
				while(true){
					
					Calendar c = Calendar.getInstance();
					int MINUTE = c.get(Calendar.MINUTE);
					int SECOND = c.get(Calendar.SECOND);

					if (MINUTE % 5 == 0 && SECOND == 30) {// 延迟5分钟30秒
						
						Map<String,List<Object[]>> eventval,pvval;
						
						synchronized(this){
							
							eventval = eventMap;
							
							pvval = pvMap;
							
							eventMap = new HashMap<String,List<Object[]>>();
							
							pvMap = new HashMap<String, List<Object[]>>();
							
							logger.info("eventval:{},pvval:{}",eventval.size(),pvval.size());
						}
						
						
						/*
						 * 根据渠道合并开户数
						 */
						Map<String,Object[]> tempEvent = new HashMap<String,Object[]>();
						
						for(String channel : eventval.keySet()){
							
							List<Object[]> vals = eventval.get(channel);
							
							long openacctSum = 0;
							
							Object[] originalArr = vals.get(0);
							
							for(Object[] obj : vals){
								
								openacctSum += (Long)obj[1];//取出开户数，累加
							}
							
							Object[] temArr = new Object[ARRAY_LEN];
							temArr[0] = channel;
							temArr[1] = 0l;
							temArr[2] = 0l;
							temArr[3] = 0l;
							temArr[4] = 0l;
							temArr[5] = 0l;
							temArr[6] = 0l;
							temArr[7] = 0l;
							temArr[8] = openacctSum;
							temArr[9] = originalArr[2]; //type
							temArr[10] = originalArr[3]; //parent
							temArr[11] = originalArr[4];//level
							temArr[12] = originalArr[5]; //ts
							
							tempEvent.put(channel, temArr);
						}
						
						logger.info("sum event:{}",tempEvent.size());
						
						
						//存储拼接后的pv
						Map<String,Object[]> tempPV = new HashMap<String,Object[]>();
						
						
						for(String channel : pvval.keySet()){
							
							List<Object[]> vals = pvval.get(channel);
							
							long pvSum = 0,uvSum = 0,enterSum = 0,hongdongindexSum = 0,
									openacctindexSum = 0,authindexSum = 0,openacctresultindexSum = 0;
							
							for(Object[] obj : vals){
								
								pvSum += (Long)obj[1];//取出开户数，累加
								uvSum += (Long)obj[2];
								enterSum += (Long)obj[3];
								hongdongindexSum += (Long)obj[4];
								openacctindexSum += (Long)obj[5];
								authindexSum += (Long)obj[6];
								openacctresultindexSum += (Long)obj[7];
								
							}
							
							
							Object[] orginalArr = vals.get(0);
							
							Object[] tempArr = new Object[ARRAY_LEN];
							tempArr[0] = channel;
							tempArr[1] = pvSum;
							tempArr[2] = uvSum;
							tempArr[3] = enterSum;
							tempArr[4] = hongdongindexSum;
							tempArr[5] = openacctindexSum;
							tempArr[6] = authindexSum;
							tempArr[7] = openacctresultindexSum;
							tempArr[8] = tempEvent.get(channel) == null ? 0l : tempEvent.get(channel)[8];//开户数
							tempArr[9] = orginalArr[8]; //type
							tempArr[10] = orginalArr[9]; //parent
							tempArr[11] = orginalArr[10];//level
							tempArr[12] = orginalArr[11]; //ts
							
							tempPV.put(channel, tempArr);
							
							logger.info("tempArr:{}",tempArr);
							
							tempEvent.remove(channel);
						}
						
						//eventMap与pvMap的并集
						tempPV.putAll(tempEvent);
						logger.info("sum union:{}",tempPV.size());
						
						eventAccountDao.insertH5pvAndEvent(new ArrayList(tempPV.values()));
						
						logger.info("into db :{}",tempPV.size());
						
						try {
							TimeUnit.MINUTES.sleep(5);;
						} catch (InterruptedException e) {
							
						}
						
					}
					
				}
				
			}}.start();
	}

	@Override
	public void execute(Tuple input) {
		
		String source = input.getStringByField("source");
		
		String channel = input.getStringByField("channel");
		
		Object[] data = (Object[])input.getValueByField("channeldata");
		
		logger.info("source:{},channel:{},data:{}",source,channel,data);
		
		
		if(HbConstants.APP_EVENT.equals(source)){
			
			synchronized(this){
				
				List<Object[]> datas = eventMap.get(channel);
				
				if(datas == null){
					
					datas = new ArrayList<Object[]>();
					eventMap.put(channel, datas);
				}
					
				datas.add(data);
				
			}
			
			logger.info("eventMap_inner:{}",eventMap.size());
			
		}else if(HbConstants.APP_PV.equals(source)){
			
			synchronized(this){
				
				List<Object[]> datas = pvMap.get(channel);
				
				if(datas == null){
					
					datas = new ArrayList<Object[]>();
					pvMap.put(channel, datas);
				}
					
				datas.add(data);
				
			}
			
			logger.info("pvMap_inner:{}",pvMap.size());
			
			
		}else{
			
			logger.warn("invalid source");
		}
		
		collector.ack(input);
		

	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		
	}
	
}
