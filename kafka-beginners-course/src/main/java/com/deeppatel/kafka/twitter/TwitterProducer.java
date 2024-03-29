package com.deeppatel.kafka.twitter;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;

public class TwitterProducer {

	Logger logger = LoggerFactory.getLogger(TwitterProducer.class.getName());
	
	private String consumerKey = "c6WqiDlp8aORaRrvc6ra6vZoV";
	private String consumerSecret = "6BExxYUrR2lI5KeNx8l8wNRk0TKmIWovlw)lnl63eEbMsJcMjx";
	private String token = "1429688634-A7bzxqW6f17B9xACxKhvb42YxmnXAA6V4lDi5wK";
	private String secret = "L5UcM3H2Yn1lscZ8WUmhimM2EeFNFpUMoBrVzaFCDVmq8";

	public TwitterProducer() {
	}

	public static void main(String[] args) {
		new TwitterProducer().run();

	}

	public void run() {

		/**
		 * Set up your blocking queues: Be sure to size these properly based on expected
		 * TPS of your stream
		 */
		BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(1000);

		// create a twitter client
		Client client = createTwitterClient(msgQueue);

		// Attempts to establish a connection.
		client.connect();

		// create a kafka producer

		// loop to send tweets to kafka
		// on a different thread, or multiple different threads....
		while (!client.isDone()) {
			String msg = null;
			try {
				msgQueue.poll(5, TimeUnit.SECONDS);
			} catch (InterruptedException e) {
				e.printStackTrace();
				client.stop();
			}
			if (msg != null) {
				logger.info(msg);
			}
		}
		logger.info("End of application");
	}

	public Client createTwitterClient(BlockingQueue<String> msgQueue) {

		/**
		 * Declare the host you want to connect to, the endpoint, and authentication
		 * (basic auth or oauth)
		 */
		Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
		StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();

		List<String> terms = Lists.newArrayList("bitcoin");
		hosebirdEndpoint.trackTerms(terms);

		// These secrets should be read from a config file
		Authentication hosebirdAuth = new OAuth1(consumerKey, consumerSecret, token, secret);

		ClientBuilder builder = new ClientBuilder().name("Hosebird-Client-01") // optional: mainly for the logs
				.hosts(hosebirdHosts).authentication(hosebirdAuth).endpoint(hosebirdEndpoint)
				.processor(new StringDelimitedProcessor(msgQueue));

		Client hosebirdClient = builder.build();
		return hosebirdClient;

	}

}
