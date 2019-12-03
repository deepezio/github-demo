package com.deeppatel.kafka_beginners_course;

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

public class ProducerDemoKeys {

	public static void main(String[] args) throws InterruptedException, ExecutionException {
		final Logger logger = LoggerFactory.getLogger(ProducerDemoKeys.class);
		//create Producer properties
		Properties properties = new Properties();
		String bootstrapServers = "127.0.0.1:9092";
		properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		
		//create the producer
		KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
		
		for(int i = 0; i < 10; i++) {
		//create a producer record
			
			String topic = "first_topic";
			String value = "Hello World!" + Integer.toString(i);
			String key = "id_" + Integer.toString(i);
			
		ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, key, value);
		
		logger.info("Key:" + key); //log the key
		
		//send data - asynchronous
		producer.send(record, new Callback() {
			
			public void onCompletion(RecordMetadata metadata, Exception exception) {
				//executes every time record is successfully sent or exception is thrown
				if(exception == null) {
					//the record was successfully sent
					logger.info("Received new metadata: \n" + 
					"Topic:" + metadata.topic() + "\n" + 
					"Partititon:" + metadata.partition() + "\n" +
					"Offset:" + metadata.offset() + "\n" + 
					"Timestamp:" + metadata.timestamp());
				}
				else {
					logger.error("Error while producing",exception);
					
				}
			}
		}).get(); //block the send to make it synchronus - don't do this in production!
	}
		//flush data
		producer.flush();
		
		//flush and close producer
		producer.close();
	}

}
