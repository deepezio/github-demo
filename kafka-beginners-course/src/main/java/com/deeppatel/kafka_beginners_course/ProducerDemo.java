package com.deeppatel.kafka_beginners_course;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

public class ProducerDemo {
	
	public static void main(String[] args) {
		
		//create Producer properties
		Properties properties = new Properties();
		String bootstrapServers = "127.0.0.1:9092";
		properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		
		//create the producer
		KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
		
		//create a producer record
		ProducerRecord<String, String> record = new ProducerRecord<String, String>("first_topic", "Hello World!");
		
		//send data - asynchronus
		producer.send(record);
		
		//flush data
		producer.flush();
		
		//flush and close producer
		producer.close();
	}

}
