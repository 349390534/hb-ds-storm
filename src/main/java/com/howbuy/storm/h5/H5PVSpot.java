/**
 * 
 */
package com.howbuy.storm.h5;

import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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
 * @author yichao.song
 * 
 * 
 * 瀹炴椂鑾峰彇H5娴侀噺鏁版嵁
 * 
 */
public class H5PVSpot extends BaseRichSpout {

	/**
	 * 
	 */
	private static final long serialVersionUID = 2615671380793354709L;

	private Logger logger = LoggerFactory.getLogger(getClass());

	private volatile BlockingQueue<String> pvQueue = new LinkedBlockingQueue<String>();

	SpoutOutputCollector _collector;
	
	//desturl howbuy鍖归厤
	private static Pattern HOWBUY_DOMAIN_PATTERN = Pattern
				.compile("(?:http|https)://((?!m|mzt|zt|data).*)\\.(?:howbuy|ehowbuy)");
	
	
	@SuppressWarnings("rawtypes")
	@Override
	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		_collector = collector;

		Properties props = new Properties();
		if(SysConfig.getConfig().isIs_dev()){
			props.put("zookeeper.connect","192.168.220.157:2181,192.168.220.155:2181");
		}else{
			props.put("zookeeper.connect","10.70.70.27:2181,10.70.70.28:2181");
		}
		props.put("group.id", "group_storm_h5pv");
		props.put("zookeeper.session.timeout.ms", "8000");
		props.put("zookeeper.sync.time.ms", "200");
		props.put("auto.commit.interval.ms", "1000");

		final ConsumerConnector consumer = kafka.consumer.Consumer
				.createJavaConsumerConnector(new ConsumerConfig(props));
		
		final String topic = "topic_webpv";
		
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
						pvQueue.offer(str);
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

		String data = (String) pvQueue.poll();
		
		if (null != data) {
			
			logger.debug("H5PVSpot nextTuple data is:{}", data);
			
			String[] as = data.split(HbConstants.FIELD_SEPARATOR);
			
			if(as.length == 27){//杩囨护鏁版嵁涓嶅畬鏁�
				
				String proid = as[22];
				
				if(HbConstants.MOBILE_PROID.equals(proid)){//杩囨护proid涓嶇瓑浜�002
					
					String desturl = as[12];
					
					Matcher destmatcher = HOWBUY_DOMAIN_PATTERN.matcher(desturl);
					
					if (!destmatcher.find()){
						
						return;
					}
					
					String guid = as[0];
					
					if(StringUtils.isEmpty(guid))
						return;
					
					_collector.emit(new Values(guid,data));
					
					logger.debug("AppActivationSpot emit data :"  + data);
					
				}
				
			}
		}
		
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("guid","data"));
	}

}
