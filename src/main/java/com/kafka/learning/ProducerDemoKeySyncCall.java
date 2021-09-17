package com.kafka.learning;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProducerDemoKeySyncCall {

	public static void main(String[] args) throws InterruptedException, ExecutionException {
		final Logger logger = LoggerFactory.getLogger(ProducerDemoKeySyncCall.class);
		
		logger.info("Entering into produer");
		// Create Producer Properties
		Properties prop = new Properties();
		prop.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
		prop.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		prop.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		// Create Producer
		KafkaProducer<String,String> kp = new KafkaProducer<String,String>(prop);
		// create producer record
	for(int i=0;i<5;i++) {
		String key="id_"+Integer.toString(i);
		String value="Java Kafka Messaging"+Integer.toString(i);
	    ProducerRecord<String,String> pr = new ProducerRecord<String,String>("first_topic",key,value);
		// send messages
            kp.send(pr,new Callback() {
				
				public void onCompletion(RecordMetadata metadata, Exception exception) {
					if(exception==null) {
						logger.info("Topic",metadata.topic());
						logger.info("Partition",metadata.partition());
						logger.info("Offset",metadata.offset());
						
						logger.info("Timestamp",metadata.timestamp());
					}
					else {
						logger.error("error in producing message",exception);
					}
					
				}
			}).get();
	}
            // flush and close
            kp.flush();
            kp.close();
            
            logger.info("Exit from produer");
	
	}

}
