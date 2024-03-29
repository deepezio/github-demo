package com.deeppatel.kafka_beginners_course;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsumerDemoWithThread {
	private Logger logger = LoggerFactory.getLogger(ConsumerDemoWithThread.class.getName());

	public static void main(String[] args) {
		new ConsumerDemoWithThread().run();
	}
	
	private ConsumerDemoWithThread() {
		
	}
	private void run() {
		String bootstrapServers = "127.0.0.1:9092";
		String groupId = "my-fifth-app";
		String topic = "first_topic";
		
		// latch for dealing with multiple threads
		CountDownLatch latch = new CountDownLatch(1);
		
		// create the consumer runnable
		logger.info("Creating the consumer thread");
		final Runnable myConsumerRunnable = new ConsumerThread(topic, bootstrapServers, groupId, latch);
		
		// start the thread
		Thread myThread = new Thread(myConsumerRunnable);
		myThread.start();
		
		// add a shutdown hook
		Runtime.getRuntime().addShutdownHook(new Thread( () -> {
			logger.info("Caught shutdown hook");
			((ConsumerThread) myConsumerRunnable).shutdown();
			try {
				latch.await();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		));
		
		try {
			latch.await();
		} catch (InterruptedException e) {
			logger.error("Application got interrupted", e);
		} finally {
			logger.info("Application is closing");
		}
	}

	public class ConsumerThread implements Runnable {

		private CountDownLatch latch;
		private KafkaConsumer<String, String> consumer;

		public ConsumerThread(String topic, String bootstrapServers, String groupId, CountDownLatch latch) {
			this.latch = latch;

			// create consumer configs
			Properties properties = new Properties();
			properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
			properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
			properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
			properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
			properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

			// create consumer
			consumer = new KafkaConsumer<String, String>(properties);

			// subscribe consumer to our topic(s)
			consumer.subscribe(Arrays.asList(topic));
		}

		public void run() {

			// poll for new data
			try {
			while (true) {
				ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));// new in Kafka 2.0.0

				for (ConsumerRecord<String, String> record : records) {
					logger.info("Key:" + record.key() + ", Value:" + record.value());
					logger.info("Partition:" + record.partition() + ", Offset:" + record.offset());
				}
			}
		} catch (WakeupException e) {
			logger.info("Received shutdown signal!");
		} finally {
			consumer.close();
			latch.countDown();
		}
	}
		
		public void shutdown() {
			consumer.wakeup();
		}

	}

}
