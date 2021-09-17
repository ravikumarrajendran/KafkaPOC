package com.kafka.learning;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsumerDemoSeekAssign {

	public static void main(String[] args) {
final Logger logger = LoggerFactory.getLogger(ConsumerDemoSeekAssign.class);
		
		logger.info("Entering into Consumer");
		// Create Consumer Config
		Properties prop = new Properties();
		prop.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"127.0.0.1:9092");
		prop.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
		prop.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
		//prop.setProperty(ConsumerConfig.GROUP_ID_CONFIG,"my-fifth-application");
		prop.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
		
		// create consumer
		KafkaConsumer<String,String> kc = new KafkaConsumer(prop);
		
		TopicPartition tp = new TopicPartition("first_topic", 0);
		kc.assign(Arrays.asList(tp));
		kc.seek(tp, 15L);
		
		// Consumer Record
		while(true) {
			ConsumerRecords<String,String> crs = kc.poll(Duration.ofMillis(100));
			for(ConsumerRecord<String,String> cr:crs) {
				logger.info("key"+cr.key());
				logger.info("value"+cr.value());
			}
		}
		

	}

}
