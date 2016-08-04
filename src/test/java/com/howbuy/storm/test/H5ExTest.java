package com.howbuy.storm.test;

public class H5ExTest {
	static final String topic = "test_ex";
	public static void main(String[] args) {
		KafkaProducer producer = new KafkaProducer(topic);
		producer.pubMessage("testkey", "test");
	}

}
