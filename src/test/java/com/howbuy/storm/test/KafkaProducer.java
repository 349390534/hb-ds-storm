/**
 * 
 */
package com.howbuy.storm.test;

import java.util.Properties;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author qiankun.li
 * 
 */
public class KafkaProducer {

	private Logger logger = LoggerFactory.getLogger(getClass());

	private kafka.javaapi.producer.Producer<String, String> producer;

	private String topic;

	private final Properties props = new Properties();

	public KafkaProducer(String topic) {
		//此处配置的是kafka的端�?
		props.put("metadata.broker.list", "192.168.220.154:9092,192.168.220.155:9092");
		//配置value的序列化�?       
		props.put("serializer.class", "kafka.serializer.StringEncoder");   
		//配置key的序列化�?
		props.put("key.serializer.class", "kafka.serializer.StringEncoder");

		producer = new Producer<String, String>(new ProducerConfig(props));
		this.topic = topic;
	}

	public void pubMessage(String key, String message) {
		producer.send(new KeyedMessage<String, String>(topic, key, message));
//		logger.info("publish mes topic : {}, mes: {}", topic, message);
	}

}
