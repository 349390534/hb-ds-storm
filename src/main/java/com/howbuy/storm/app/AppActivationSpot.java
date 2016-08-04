/**
 * 
 */
package com.howbuy.storm.app;

import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
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
public class AppActivationSpot extends BaseRichSpout {

	/**
	 * 
	 */
	private static final long serialVersionUID = 2615671380793354709L;

	private Logger logger = LoggerFactory.getLogger(getClass());

	private volatile BlockingQueue<String> eventDataQueue = new LinkedBlockingQueue<String>();

	SpoutOutputCollector _collector;
	
	
	@SuppressWarnings("rawtypes")
	@Override
	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		_collector = collector;

		Properties props = new Properties();
		if(SysConfig.getConfig().isIs_dev()){
			props.put("zookeeper.connect","192.168.220.107:2181,192.168.20.108:2181");
		}else{
			props.put("zookeeper.connect","10.70.70.27:2181,10.70.70.28:2181");
		}
		props.put("group.id", "group_storm_activation");
		props.put("zookeeper.session.timeout.ms", "8000");
		props.put("zookeeper.sync.time.ms", "200");
		props.put("auto.commit.interval.ms", "1000");

		final ConsumerConnector consumer = kafka.consumer.Consumer
				.createJavaConsumerConnector(new ConsumerConfig(props));
		
		final String topic = "topic_appactivate";
		
		new Thread() {

			@Override
			public void run() {
				
				
				Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
			    topicCountMap.put(topic, new Integer(1));
			    Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = 
			    								consumer.createMessageStreams(topicCountMap);
			    
			    List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(topic);
			    
			    KafkaStream<byte[], byte[]> stream =  streams.get(0);
			    
			    ConsumerIterator<byte[], byte[]> it = stream.iterator();
			    
			    while (it.hasNext()) {
					try {
						String str = new String(it.next().message(),"utf-8");
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
			logger.debug("AppActivationSpot nextTuple data is:{}", data);
			//校验规则
			String[] as = data.split(HbConstants.FIELD_SEPARATOR);
			if(as!=null && as.length == 13){
				String type = as[5];
				if(StringUtils.isNotBlank(type) && "3".equals(type.trim())){
					logger.warn("param type is 3");
					return;
				}
				String proid = as[1];
				String outletcode = as[4];
				if(StringUtils.isBlank(proid)){//平台号不能为空
					logger.warn("param proid is null");
					return;
				}
				if(StringUtils.isBlank(outletcode)){//网点号不能为空
					logger.warn("param outletcode is null");
					 return;
				}
				_collector.emit(new Values(proid,outletcode,data));
				logger.debug("AppActivationSpot emit data :"  + data);
			}else{
				logger.error("AppActivationSpot invalid input data");
			}
		}
		
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("proid","outletcode","data"));
	}

}
