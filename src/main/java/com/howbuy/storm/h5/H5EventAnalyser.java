package com.howbuy.storm.h5;

import java.sql.Timestamp;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.task.OutputCollector;
import backtype.storm.tuple.Values;

import com.howbuy.storm.common.HbConstants;
import com.howbuy.storm.dao.ChannelEventAccountDao;


/**
 * H5事件分析 
 * @author yichao.song
 *
 */
public class H5EventAnalyser extends BaseAnly {
	
	public H5EventAnalyser(OutputCollector outputCollector) {
		super();
		this.outputCollector = outputCollector;
	}


	private OutputCollector outputCollector;
	
	private static Logger logger = LoggerFactory.getLogger(H5EventAnalyser.class);
	
	private static ChannelEventAccountDao eventAccountDao = new ChannelEventAccountDao();
	
	private static String OPEN_ACCT = "openacct";
	
	//渠道
	private static String[] channels = new String[] { ALL_SEARCH_CHANNEL,
		ALL_TUIGUANG_CHANNEL, ALL_OTHER_CHANNEL, ALL_DIRECT_CHANNEL };
	
	//指标
	private static String[] BIZS = new String[] {OPEN_ACCT};
	
	private static Map<String,String> TYPE_BIZ = new HashMap<String,String>(){
		
		{
			
			put("4",OPEN_ACCT);
		}
	};
	
	/**
	 * 指标渠道明细
	 * @return
	 */
	private Map<String,Map<String,Long>> buildBizMap(){
		
		Map<String,Map<String,Long>> map = new HashMap<String, Map<String,Long>>();
		
		for(int i = 0; i < BIZS.length; i++){
			
			map.put(BIZS[i], new HashMap<String,Long>());
		}
		
		return map;
	}
	
	/**
	 * 渠道指标映射
	 * @return
	 */
	private Map<String,Map<String,Long>> buildChannelMap(Map<String,Map<String,Long>> source){
		
		Map<String,Map<String,Long>> s = source;
		
		if(s == null)
			s = new HashMap<String,Map<String,Long>>();
		
		for (int i = 0; i < channels.length; i++) {

			Map<String,Long> stat = new HashMap<String,Long>();

			s.put(channels[i], stat);

			for (int j = 0; j < BIZS.length; j++) {

				stat.put(BIZS[j], new Long(0));
			}
		}
		
		return s;
	}
	
	
	/**
	 * 计算
	 * @param vals
	 */
	public void cal(List<String> vals,Timestamp ts){
		
		if(vals.size() == 0)
			return;
		
		//“全部”渠道汇总
		Map<String,Long> allbizChannelMap = new HashMap<String,Long>(){

			{
				for(int i = 0; i < BIZS.length; i++){
					
					put(BIZS[i], 0l);
				}
			}
		};
		
		//渠道汇总指标映射
		Map<String,Map<String,Long>> channel2bizMap = buildChannelMap(null);
		
		//业务渠道明细指标映射
		Map<String,Map<String,Long>> biz2channelMap = buildBizMap();
		
		
		for(String value : vals){
			
			//当前渠道
			String currChannel = "";
			
			String[] linearr = value.split(FIELD_SEPARATOR);

			String type = linearr[1];
			
			
			if(!"4".equals(type)){//开户
				
				logger.warn("valid type:{}",type);
				
				continue;
			}

			String otrack = linearr[20].trim();

			if (!isEmpty(otrack)) {//有otrack

				Matcher m = OTRACK_PATTERN.matcher(otrack);

				if (!m.matches()) {// otrack格式不合法

					currChannel = OTHER_CHANNEL;

					logger.warn("invalid otrack:{},len:{}",otrack,otrack.length());

				} else {//合法otrack

					String[] arr = otrack.split("\\.");

					String[] body = arr[0].split("-");

					String htag = body[0];

					if (htag.equals("0")) {// 站内直接访问

						htag = DIRECT_CHANNEL;

					}
					
					// htag合法性判断
					else if(!TUI_GUANG_HTAG_PATTERN.matcher(htag).matches() &&
							!SEARCH_PATTERN.matcher(htag).matches() &&
							!DIRECT_CHANNEL.equals(htag) &&
							!OTHER_CHANNEL.equals(htag))
						
						currChannel = OTHER_CHANNEL;
					else
						currChannel = htag;
				}


			} else{//无otrack

				if(isEmpty(currChannel))
					currChannel = DIRECT_CHANNEL;

			}
			
			String biz = TYPE_BIZ.get(type);
			
			Map<String,Long> bizMap = null;
			
			if (DIRECT_CHANNEL.equals(currChannel)){// 直接渠道
				
				String firstClass = currChannel;
				
				Map<String,Long> channelmap = biz2channelMap.get(biz);
				
				getAndIncr(channelmap, firstClass, new Long(1));
				
				bizMap = channel2bizMap.get(ALL_DIRECT_CHANNEL);
				
			}else if(SEARCH_PATTERN.matcher(currChannel).matches()){ // 搜索渠道
				
				String firstClass = currChannel;
				
				Map<String,Long> channelmap = biz2channelMap.get(biz);
				
				getAndIncr(channelmap, firstClass, new Long(1));
				
				bizMap = channel2bizMap.get(ALL_SEARCH_CHANNEL);
				
				
			}else if(OTHER_CHANNEL.equals(currChannel)){//其他渠道
				
				String firstClass = currChannel;
				
				Map<String,Long> channelmap = biz2channelMap.get(biz);
				
				getAndIncr(channelmap, firstClass, new Long(1));
				
				bizMap = channel2bizMap.get(ALL_OTHER_CHANNEL);
				
				
			}else if (TUI_GUANG_HTAG_PATTERN.matcher(currChannel).matches()) {// 其他渠道
				
				
				Map<String,Long> channelmap = biz2channelMap.get(biz);
				
				// 解析三级渠道
				String firstClass = currChannel.substring(0, 3);

				String secondClass = currChannel.substring(0, 6);

				String thirdClass = currChannel.substring(0, 10);
				
				getAndIncr(channelmap, firstClass, new Long(1));
				getAndIncr(channelmap, secondClass, new Long(1));
				getAndIncr(channelmap, thirdClass, new Long(1));
				
				bizMap = channel2bizMap.get(ALL_TUIGUANG_CHANNEL);
			}
			
			getAndIncr(bizMap, biz, new Long(1));
			
		}
		
		/*********************汇总*****************************/
		
		//“全部”渠道汇总
		for(String channel : channel2bizMap.keySet()){
			
			Map<String,Long> bizmap = channel2bizMap.get(channel);
			
			for(int i = 0; i < BIZS.length; i++){
				
				Long openacctnum = bizmap.get(BIZS[i]) == null ? new Long(0) : bizmap.get(BIZS[i]);
				
				getAndIncr(allbizChannelMap, BIZS[i], openacctnum);
				
			}
			
		}
		
		
		/*********************汇总结束 *****************************/
		
		
		//明细入库
		Map<String,Long> openacctMap = biz2channelMap.get(OPEN_ACCT);
		
		for(String channel : openacctMap.keySet()){
			
			long openacctnum = openacctMap.get(channel) == null ? 0 : openacctMap.get(channel);
			
			int type = getType(channel);

			String root = "-1";

			String level = null;

			switch (type) {
			case 1:
				level = "1";
				break;

			case 2:
				level = "1";
				break;

			case 3:
				level = "1";
				break;

			case 4:
				level = channel.length() == 3 ? "1"
						: channel.length() == 6 ? "2"
								: channel.length() == 10 ? "3" : "-1";

				root = level.equals("1") ? "-1"
						: level.equals("2") ? channel.substring(0, 3)
								: channel.substring(0, 6);
				break;

			default:
				root = "-1";
				level = "-1";

			}
			
			Object[] params = {channel,openacctnum,type,root,level,ts};
			
			outputCollector.emit(new Values(HbConstants.APP_EVENT,channel,params));
		}
		
		//各个渠道汇总入库
		for(String channel : channel2bizMap.keySet()){
			
			Map<String,Long> bizMap = channel2bizMap.get(channel);
			
			Long openacctnum = bizMap.get(OPEN_ACCT) == null ? 0 : bizMap.get(OPEN_ACCT);
			
			Object[] params = {channel,openacctnum,"5","-1","1",ts};
			
			outputCollector.emit(new Values(HbConstants.APP_EVENT,channel,params));
			
		}
		
		
		//“全部”渠道入库
		Object[] param = {ALL_CHANNEL,
				allbizChannelMap.get(OPEN_ACCT)==null ? 0 :allbizChannelMap.get(OPEN_ACCT),
				"5","-1","1",ts};
		
		outputCollector.emit(new Values(HbConstants.APP_EVENT,ALL_CHANNEL,param));
	}

}
