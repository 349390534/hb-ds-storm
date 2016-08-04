/**
 * 
 */
package com.howbuy.storm.app;

import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.Map.Entry;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.howbuy.storm.common.HbConstants;
import com.howbuy.storm.common.SysConfig;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

/**
 * @author qiankun.li
 * 
 */
public class AppEventSpot extends BaseRichSpout {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1694002883714745453L;

	private Logger logger = LoggerFactory.getLogger(getClass());

	private volatile BlockingQueue<String> eventDataQueue = new LinkedBlockingQueue<String>();

	SpoutOutputCollector _collector;

	@SuppressWarnings("rawtypes")
	@Override
	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		_collector = collector;

		Properties props = new Properties();
		if (SysConfig.getConfig().isIs_dev()) {
			props.put("zookeeper.connect","192.168.220.107:2181,192.168.20.108:2181");
		} else {
			props.put("zookeeper.connect", "10.70.70.27:2181,10.70.70.28:2181");
		}
		props.put("group.id", "group_storm_appevent");
		props.put("zookeeper.session.timeout.ms", "8000");
		props.put("zookeeper.sync.time.ms", "200");
		props.put("auto.commit.interval.ms", "1000");

		final ConsumerConnector consumer = kafka.consumer.Consumer
				.createJavaConsumerConnector(new ConsumerConfig(props));

		final String topic = "topic_appevent";

		new Thread() {

			@Override
			public void run() {

				Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
				topicCountMap.put(topic, new Integer(1));
				Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer
						.createMessageStreams(topicCountMap);

				List<KafkaStream<byte[], byte[]>> streams = consumerMap
						.get(topic);

				KafkaStream<byte[], byte[]> stream = streams.get(0);

				ConsumerIterator<byte[], byte[]> it = stream.iterator();

				while (it.hasNext()) {

					try {
						String str = new String(it.next().message(), "utf-8");
						eventDataQueue.offer(str);
						logger.debug("offer data:{}", str);
					} catch (UnsupportedEncodingException e) {
						logger.error("err:{}", e);
					}
				}
			}

		}.start();

	}

	@Override
	public void nextTuple() {

		String data = (String) eventDataQueue.poll();
		if (null != data) {
			logger.debug("AppEventSpot nextTuple data is:{}", data);
			String[] as = data.split(HbConstants.FIELD_SEPARATOR);
			if (as != null && as.length == 21) {
				String type = as[1];
				if (StringUtils.isBlank(type)) {
					logger.warn("param type is null");
					return;
				}
				String proid = as[19];
				if (StringUtils.isBlank(proid)) {
					logger.warn("param proid is null");
					return;
				}
				// 判断网点类型 coopId
				String outletcode = as[13];
				if (StringUtils.isBlank(outletcode)) {
					logger.warn("param outletcode is null");
					return;
				}
				String proidC = convertProid(proid, outletcode);
				if(StringUtils.isBlank(proidC)){
					logger.info("AppEventSpot data:{}",data);
					logger.error("AppEventSpot proid:{}, outletcode:{} can not convert",proid, outletcode);
					return;
				}
				logger.debug("APPeventSpot proid:{},outletcode:{},convert proid:{}",proid, outletcode, proidC);
				data = data.replace(proid, proidC);
				_collector.emit(new Values(proidC, outletcode, data));
				logger.debug("AppEventSpot emit data :" + data);
			} else {
				logger.error("AppEventSpot invalid input data");
			}
		}

	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("proid", "outletcode", "data"));

	}

	// 激活数据和APP事件上报数据的匹配
	private HashMap<String, HashMap<String, String>> initMap = new HashMap<String, HashMap<String, String>>();
	{
		// 掌基
		HashMap<String, String> map = new HashMap<String, String>();
		map.put("A20120701", "3001"); //A20140313
		map.put("H20131104", "3001");
		map.put("-1", "3002");// 其他
		initMap.put("28294488", map);
		// 储蓄罐
		HashMap<String, String> map2 = new HashMap<String, String>();
		map2.put("A20140301", "2001");
		map2.put("-1", "2002");// 其他
		initMap.put("105975974", map2);

	}
	private String convertProid(String proid, String outletcode) {
		String proidC = null;
		Set<Entry<String, HashMap<String, String>>> map = initMap.entrySet();
		for (Entry<String, HashMap<String, String>> en : map) {
			String key = en.getKey();
			if (key.equals(proid)) {
				HashMap<String, String> mapVar = en.getValue();
				proidC = mapVar.get(outletcode);
				break;
			}
		}
		if (null == proidC) {
			HashMap<String, String> mapin = initMap.get(proid);
			if (null != mapin && !mapin.isEmpty()) {
				proidC = mapin.get("-1");
			}
		}
		return proidC;
	}
	
	 
	
}
