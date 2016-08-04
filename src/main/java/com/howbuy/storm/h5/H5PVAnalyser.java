package com.howbuy.storm.h5;

import java.io.IOException;
import java.net.URISyntaxException;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.task.OutputCollector;
import backtype.storm.tuple.Values;

import com.howbuy.storm.common.HbConstants;

public class H5PVAnalyser extends BaseAnly {
	
	public H5PVAnalyser(OutputCollector outputCollector) {
		super();
		this.outputCollector = outputCollector;
	}

	private OutputCollector outputCollector;

	private static String PV = "pv";

	private static String UV = "uv";

	private static String HUODONG_INDEX = "huodong_index";

	private static String OPENACCT_INDEX = "openacct_index";

	private static String AUTH_INDEX = "auth_index";

	private static String OPENACCT_RESULT = "openacct_result";

	private static Logger logger = LoggerFactory.getLogger(H5PVAnalyser.class);

	// pageid位数与页面映射
	private static Map<String, String> pageid_suffix_map = new HashMap<String, String>() {
		{
			put("6", HUODONG_INDEX);
			put("21000", OPENACCT_INDEX);
			put("21010", AUTH_INDEX);
			put("21020", OPENACCT_RESULT);
		}
	};

	// 渠道
	private static String[] channels = new String[] { ALL_SEARCH_CHANNEL,
			ALL_TUIGUANG_CHANNEL, ALL_OTHER_CHANNEL, ALL_DIRECT_CHANNEL };

	// 指标
	private static String[] bizs = new String[] { PV, UV, HUODONG_INDEX,
			OPENACCT_INDEX, AUTH_INDEX, OPENACCT_RESULT };

	/**
	 * 指标渠道明细
	 * 
	 * @return
	 */
	private Map<String, Map<String, Long>> buildBizMap() {

		Map<String, Map<String, Long>> map = new HashMap<String, Map<String, Long>>();

		for (int i = 0; i < bizs.length; i++) {

			map.put(bizs[i], new HashMap<String, Long>());
		}

		return map;
	}

	/**
	 * 渠道指标映射
	 * 
	 * @return
	 */
	private Map<String, Map<String, Long>> buildChannelMap(
			Map<String, Map<String, Long>> source) {

		Map<String, Map<String, Long>> s = source;

		if (s == null)
			s = new HashMap<String, Map<String, Long>>(0);

		for (int i = 0; i < channels.length; i++) {
			Map<String, Long> stat = new HashMap<String, Long>(bizs.length);
			s.put(channels[i], stat);
			for (int j = 0; j < bizs.length; j++) {
				stat.put(bizs[j], new Long(0));
			}
		}
		return s;
	}

	/**
	 * 计算
	 * 
	 * @param map
	 * @throws IOException
	 * @throws InterruptedException
	 */
	public void cal(Map<String, List<String>> map, Timestamp ts)
			throws IOException, InterruptedException {

		// logger.info("pvanlymsg:{}",map);

		if (map.size() == 0)
			return;

		// “全部”渠道汇总
		Map<String, Long> allbizChannelMap = new HashMap<String, Long>() {

			{
				for (int i = 0; i < bizs.length; i++) {

					put(bizs[i], 0l);
				}
			}
		};

		// 渠道汇总指标映射
		Map<String, Map<String, Long>> channel2bizMap = buildChannelMap(null);

		// 业务渠道明细指标映射
		Map<String, Map<String, Long>> biz2channelMap = buildBizMap();

		// 进入次数
		Map<String, Long> channelEntersMap = new HashMap<String, Long>();

		for (String guid : map.keySet()) {

			List<String> datas = map.get(guid);

			// 临时渠道、指标映射
			Map<String, Map<String, Long>> tempchannel2bizMap = buildChannelMap(null);

			// 临时指标、明细渠道映射
			Map<String, Map<String, Long>> tempbiz2channelMap = buildBizMap();

			// 临时 总渠道指标
			Map<String, Long> tempallChannebizlMap = new HashMap<String, Long>() {

				{
					for (int i = 0; i < bizs.length; i++) {

						put(bizs[i], 0l);
					}
				}
			};

			boolean alreadyDirect = false;

			String currChannel = "";

			String firstClass = "";

			String secondClass = "";

			String thirdClass = "";

			for (String line : datas) {

				String[] arr = line.split(FIELD_SEPARATOR);

				String htag = arr[10];

				String srcurl = arr[2];

				String pageid = arr[18];

				logger.debug("pvinfo htag:{},srcurl:{},pageid:{}", htag, srcurl,
						pageid);

				// 分析开始
				if (!isEmpty(htag)) {// 有htag

					Matcher m = HTAG_PATTERN.matcher(htag);

					if (!m.matches()) {// 不合法格式htag

						logger.warn("invalid htag:{}", htag);

						if (isEmpty(srcurl)) {//没有ref
							
							// 直接访问
							currChannel = DIRECT_CHANNEL;

							firstClass = DIRECT_CHANNEL;

							getAndIncr(channelEntersMap, DIRECT_CHANNEL,
									new Long(1));

							getAndIncr(channelEntersMap,
									ALL_DIRECT_CHANNEL, new Long(1));
							
						} else {

							// 走ref
							String tmpRef = srcurl.trim();

							try {

								if (!INTERNAL_HOWBUY_DOMAIN_PATTERN
										.matcher(tmpRef).find()) {// ref站外

									String tempchannel = getChannel(tmpRef);

									if (tempchannel == null) {

										// 其他渠道
										currChannel = OTHER_CHANNEL;

										firstClass = OTHER_CHANNEL;

										getAndIncr(channelEntersMap,
												ALL_OTHER_CHANNEL,
												new Long(1));

									} else {

										boolean isFromSearch = searches
												.containsKey(tempchannel);

										if (isFromSearch) {

											currChannel = tempchannel;

											getAndIncr(
													channelEntersMap,
													ALL_SEARCH_CHANNEL,
													new Long(1));

										} else {

											currChannel = OTHER_CHANNEL;

											getAndIncr(
													channelEntersMap,
													ALL_OTHER_CHANNEL,
													new Long(1));
										}

										firstClass = tempchannel;
									}

									getAndIncr(channelEntersMap,
											firstClass, new Long(1));

								}

							} catch (URISyntaxException e) {

								logger.warn("invalid ref:{}" + tmpRef);

								firstClass = OTHER_CHANNEL;

								currChannel = OTHER_CHANNEL;

								getAndIncr(channelEntersMap,
										OTHER_CHANNEL, new Long(1));

								getAndIncr(channelEntersMap,
										ALL_OTHER_CHANNEL, new Long(1));

							}

						}

					} else {// htag合法
						
						if(isEmpty(srcurl)){//ref为空
							
							String flag = htag.split("\\.")[0];
							
							if (!flag.equals("0")) {// 非0级htag
								
								// 直接访问
								currChannel = DIRECT_CHANNEL;

								firstClass = DIRECT_CHANNEL;

								getAndIncr(channelEntersMap, DIRECT_CHANNEL,
										new Long(1));

								getAndIncr(channelEntersMap,
										ALL_DIRECT_CHANNEL, new Long(1));
								
							}else{
								
								//渠道
								String entire = htag.split("\\.")[1];
								
								if (TUI_GUANG_HTAG_PATTERN.matcher(entire)
										.matches()) {// 推广
									
									firstClass = entire.substring(0, 3);
									
									secondClass = entire.substring(0, 6);
									
									thirdClass = entire.substring(0, 10);
									
									getAndIncr(channelEntersMap, firstClass,
											new Long(1));
									getAndIncr(channelEntersMap, secondClass,
											new Long(1));
									getAndIncr(channelEntersMap, thirdClass,
											new Long(1));
									
									getAndIncr(channelEntersMap,
											ALL_TUIGUANG_CHANNEL, new Long(1));
									
									currChannel = entire;
									
								} else {// 非法0级htag
									
									logger.warn("invalid zero htag:{}:", htag);
								}
								
							}


						}else /*if (!INTERNAL_HOWBUY_DOMAIN_PATTERN.matcher(
										srcurl).find())*/ {//站外ref

							String flag = htag.split("\\.")[0];

							if (!flag.equals("0")) {// 站内htag

								// 走ref

								String tmpRef = srcurl.trim();

								try {

									String tempchannel = getChannel(tmpRef);

									if (tempchannel != null) {

										boolean isFromSearch = searches
												.containsKey(tempchannel);

										if (isFromSearch) {

											currChannel = tempchannel;

											getAndIncr(channelEntersMap,
													ALL_SEARCH_CHANNEL,
													new Long(1));

										} else {

											currChannel = OTHER_CHANNEL;

											getAndIncr(channelEntersMap,
													ALL_OTHER_CHANNEL,
													new Long(1));
										}

										firstClass = tempchannel;

									} else {

										// 其他渠道
										currChannel = OTHER_CHANNEL;

										firstClass = OTHER_CHANNEL;

										getAndIncr(channelEntersMap,
												ALL_OTHER_CHANNEL, new Long(1));

									}

									getAndIncr(channelEntersMap, firstClass,
											new Long(1));

								} catch (URISyntaxException e) {// uri解析错误归属其他渠道

									logger.warn("valid ref:{}", tmpRef);

									// 其他渠道
									currChannel = OTHER_CHANNEL;

									firstClass = OTHER_CHANNEL;

									getAndIncr(channelEntersMap, OTHER_CHANNEL,
											new Long(1));

									getAndIncr(channelEntersMap,
											ALL_OTHER_CHANNEL, new Long(1));

								}

							} else {// 0级htag

								String entire = htag.split("\\.")[1];

								if (TUI_GUANG_HTAG_PATTERN.matcher(entire)
										.matches()) {// 推广

									firstClass = entire.substring(0, 3);

									secondClass = entire.substring(0, 6);

									thirdClass = entire.substring(0, 10);

									getAndIncr(channelEntersMap, firstClass,
											new Long(1));
									getAndIncr(channelEntersMap, secondClass,
											new Long(1));
									getAndIncr(channelEntersMap, thirdClass,
											new Long(1));

									getAndIncr(channelEntersMap,
											ALL_TUIGUANG_CHANNEL, new Long(1));

									currChannel = entire;

								} else {// 非法0级htag

									logger.warn("invalid zero htag:{}:", htag);
								}

							}

						}

					}

				} else {// 没有htag
					
					if(isEmpty(srcurl)){//ref空
						
						// 直接访问
						currChannel = DIRECT_CHANNEL;

						firstClass = DIRECT_CHANNEL;

						getAndIncr(channelEntersMap, DIRECT_CHANNEL,
								new Long(1));

						getAndIncr(channelEntersMap,
								ALL_DIRECT_CHANNEL, new Long(1));
						
						
					}else if (!INTERNAL_HOWBUY_DOMAIN_PATTERN.matcher(srcurl).find()) {// 站外ref

						String tmpRef = srcurl.trim();

						try {

							String tempchannel = getChannel(tmpRef);

							if (tempchannel != null) {

								boolean isFromSearch = searches
										.containsKey(tempchannel);

								if (isFromSearch) {

									currChannel = tempchannel;

									getAndIncr(channelEntersMap,
											ALL_SEARCH_CHANNEL, new Long(1));

								} else {

									currChannel = OTHER_CHANNEL;

									getAndIncr(channelEntersMap,
											ALL_OTHER_CHANNEL, new Long(1));
								}

								firstClass = tempchannel;

							} else {

								// 其他渠道
								currChannel = OTHER_CHANNEL;

								firstClass = OTHER_CHANNEL;

								getAndIncr(channelEntersMap, ALL_OTHER_CHANNEL,
										new Long(1));

							}

							getAndIncr(channelEntersMap, firstClass,
									new Long(1));

						} catch (URISyntaxException e) {// uri解析错误归属其他渠道

							logger.warn("valid ref:{}", tmpRef);

							// 其他渠道
							currChannel = OTHER_CHANNEL;

							firstClass = OTHER_CHANNEL;

							getAndIncr(channelEntersMap, OTHER_CHANNEL,
									new Long(1));

							getAndIncr(channelEntersMap, ALL_OTHER_CHANNEL,
									new Long(1));

						}

					}

				}

				if (isEmpty(currChannel)) {

					// 直接访问
					currChannel = DIRECT_CHANNEL;

					firstClass = DIRECT_CHANNEL;

					getAndIncr(channelEntersMap, DIRECT_CHANNEL, new Long(1));

					getAndIncr(channelEntersMap, ALL_DIRECT_CHANNEL,
							new Long(1));

					// 首次直接访问
					alreadyDirect = true;

				}

				// 当前渠道指标
				Map<String, Long> tempchanneldetailMap = null;

				Map<String, Long> pvdetailmap = biz2channelMap.get(PV);

				Map<String, Long> tempuvdetailmap = tempbiz2channelMap.get(UV);

				// 判断pageid位数属于那个指标(biz)

				String currBiz = null;

				if (!isEmpty(pageid)) {

					for (String key : pageid_suffix_map.keySet()) {

						if (pageid.endsWith(key)) {

							currBiz = pageid_suffix_map.get(key);

							tempchanneldetailMap = tempbiz2channelMap
									.get(currBiz);

							break;
						}
					}
				}

				/*
				 * 渠道指标汇总
				 */
				Map<String, Long> tempbizMap = null;

				if (currChannel.equals(DIRECT_CHANNEL)) {// 直接渠道

					tempbizMap = tempchannel2bizMap.get(ALL_DIRECT_CHANNEL);

					getAndIncr(pvdetailmap, firstClass, new Long(1));

					getAndIncr(tempuvdetailmap, firstClass, new Long(1));

					if (null != tempchanneldetailMap)
						getAndIncr(tempchanneldetailMap, firstClass,
								new Long(1));

				} else if (SEARCH_PATTERN.matcher(currChannel).matches()) {// 搜索渠道/其他

					tempbizMap = tempchannel2bizMap.get(ALL_SEARCH_CHANNEL);

					getAndIncr(pvdetailmap, firstClass, new Long(1));

					getAndIncr(tempuvdetailmap, firstClass, new Long(1));

					if (null != tempchanneldetailMap)
						getAndIncr(tempchanneldetailMap, firstClass,
								new Long(1));

				} else if (currChannel.equals(OTHER_CHANNEL)) {// 其他渠道

					tempbizMap = tempchannel2bizMap.get(ALL_OTHER_CHANNEL);

					getAndIncr(pvdetailmap, firstClass, new Long(1));

					getAndIncr(tempuvdetailmap, firstClass, new Long(1));

					if (null != tempchanneldetailMap)
						getAndIncr(tempchanneldetailMap, firstClass,
								new Long(1));

				} else if (TUI_GUANG_HTAG_PATTERN.matcher(currChannel)
						.matches()) {// 推广渠道

					tempbizMap = tempchannel2bizMap.get(ALL_TUIGUANG_CHANNEL);

					getAndIncr(pvdetailmap, firstClass, new Long(1));
					getAndIncr(pvdetailmap, secondClass, new Long(1));
					getAndIncr(pvdetailmap, thirdClass, new Long(1));

					getAndIncr(tempuvdetailmap, firstClass, new Long(1));
					getAndIncr(tempuvdetailmap, secondClass, new Long(1));
					getAndIncr(tempuvdetailmap, thirdClass, new Long(1));

					if (null != tempchanneldetailMap) {

						getAndIncr(tempchanneldetailMap, firstClass,
								new Long(1));
						getAndIncr(tempchanneldetailMap, secondClass, new Long(
								1));
						getAndIncr(tempchanneldetailMap, thirdClass,
								new Long(1));
					}
				}

				getAndIncr(tempbizMap, PV, new Long(1));

				getAndIncr(tempbizMap, UV, new Long(1));

				if (null != currBiz) {

					getAndIncr(tempbizMap, currBiz, new Long(1));
				}

			}

			/************************* 单个用户信息汇总 *******************************************/

			// 明细汇总
			for (String biz : tempbiz2channelMap.keySet()) {

				if (biz.equals(PV))
					continue;

				Map<String, Long> tempchannelmap = tempbiz2channelMap.get(biz);

				Map<String, Long> channelmap = biz2channelMap.get(biz);

				for (String channel : tempchannelmap.keySet()) {

					// 一个指标多个pv，放入一个uv
					getAndIncr(channelmap, channel, new Long(1));
				}
			}

			// 渠道汇总
			for (String channel : tempchannel2bizMap.keySet()) {

				Map<String, Long> tempbizsmap = tempchannel2bizMap.get(channel);

				Map<String, Long> bizsmap = channel2bizMap.get(channel);

				for (String biz : tempbizsmap.keySet()) {

					// 汇总各渠道指标到"临时总渠道指标"中
					getAndIncr(tempallChannebizlMap, biz, tempbizsmap.get(biz));

					if (PV.equals(biz)) {

						// pv原数量添加
						getAndIncr(bizsmap, PV, tempbizsmap.get(biz));

					} else if (tempbizsmap.get(biz) > 0) {// 其余指标有数据增加1

						getAndIncr(bizsmap, biz, new Long(1));
					}
				}

			}

			// 全部汇总
			for (String biz : tempallChannebizlMap.keySet()) {

				if (PV.equals(biz)) {

					getAndIncr(allbizChannelMap, biz,
							tempallChannebizlMap.get(biz));

				} else {

					getAndIncr(allbizChannelMap, biz,
							tempallChannebizlMap.get(biz) > 0 ? 1l : 0l);
				}
			}

			/************************* 单个用户信息汇总结束 *******************************************/

		}

		/************************************* 传入下个bolt ***********************************************/

		// 入库参数

		// 渠道汇总入库
		long totalenter = 0;
		for (String channel : channel2bizMap.keySet()) {

			Map<String, Long> bizMap = channel2bizMap.get(channel);

			long enter = channelEntersMap.get(channel) == null ? 0
					: channelEntersMap.get(channel);

			totalenter += enter;
			
			Object[] param = { channel, bizMap.get(PV),
					bizMap.get(UV), enter, bizMap.get(HUODONG_INDEX),
					bizMap.get(OPENACCT_INDEX), bizMap.get(AUTH_INDEX),
					bizMap.get(OPENACCT_RESULT), "5", "-1", "1", ts };

			outputCollector.emit(new Values(HbConstants.APP_PV,channel,param));

		}
		
		logger.info("emit channels:{}", channel2bizMap.size());

		// “全部”渠道入库
		Object[] allparam = { ALL_CHANNEL, allbizChannelMap.get(PV),
				allbizChannelMap.get(UV), totalenter,
				allbizChannelMap.get(HUODONG_INDEX),
				allbizChannelMap.get(OPENACCT_INDEX),
				allbizChannelMap.get(AUTH_INDEX),
				allbizChannelMap.get(OPENACCT_RESULT), "5", "-1", "1", ts };
		
		outputCollector.emit(new Values(HbConstants.APP_PV,ALL_CHANNEL,allparam));
		
		logger.info("emit All:{}",allbizChannelMap.size());

		// 明细渠道入库

		Map<String, Long> pv_map = biz2channelMap.get(PV);

		Map<String, Long> uv_map = biz2channelMap.get(UV);

		Map<String, Long> huuodongindex_map = biz2channelMap.get(HUODONG_INDEX);

		Map<String, Long> openacctindex_map = biz2channelMap
				.get(OPENACCT_INDEX);

		Map<String, Long> authindex_map = biz2channelMap.get(AUTH_INDEX);

		Map<String, Long> openacctresult_map = biz2channelMap
				.get(OPENACCT_RESULT);

		for (String channel : pv_map.keySet()) {

			long pv = pv_map.get(channel);

			long uv = uv_map.get(channel) == null ? 0 : uv_map.get(channel);

			long hongdongindex = huuodongindex_map.get(channel) == null ? 0
					: huuodongindex_map.get(channel);

			long openacctindex = openacctindex_map.get(channel) == null ? 0
					: openacctindex_map.get(channel);

			long authindex = authindex_map.get(channel) == null ? 0
					: authindex_map.get(channel);

			long openacctresultindex = openacctresult_map.get(channel) == null ? 0
					: openacctresult_map.get(channel);

			long enter = channelEntersMap.get(channel) == null ? 0
					: channelEntersMap.get(channel);

			int type = getType(channel);

			String parent = "-1";

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

				parent = level.equals("1") ? "-1" : level.equals("2") ? channel
						.substring(0, 3) : channel.substring(0, 6);
				break;

			default:
				parent = "-1";
				level = "-1";

			}

			Object[] params = { channel, pv, uv, enter, hongdongindex,
					openacctindex, authindex, openacctresultindex, type, parent,
					level, ts };

			
			outputCollector.emit(new Values(HbConstants.APP_PV,channel,params));
		}
		
		logger.info("emit details:{}",pv_map.size());

	}

	public static boolean isEmpty(String val) {

		if (StringUtils.isEmpty(val) || "null".equals(val))
			return true;
		return false;
	}

}