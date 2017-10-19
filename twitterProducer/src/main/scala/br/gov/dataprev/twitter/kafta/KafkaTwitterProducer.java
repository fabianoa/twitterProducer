package br.gov.dataprev.twitter.kafta;

import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.LinkedBlockingQueue;

import twitter4j.*;
import twitter4j.conf.*;

import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.conf.ConfigurationBuilder;
import twitter4j.json.DataObjectFactory;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import kafka.producer.KeyedMessage;

/**
 * A Kafka Producer that gets tweets on certain keywords
 * from twitter datasource and publishes to a kafka topic
 * 
 * Arguments: <comsumerKey> <consumerSecret> <accessToken> <accessTokenSecret> <topic-name> <keyword_1> ... <keyword_n>
 * <comsumerKey>		- Twitter consumer key 
 * <consumerSecret>  	- Twitter consumer secret
 * <accessToken>		- Twitter access token
 * <accessTokenSecret>	- Twitter access token secret
 * <topic-name>			- The kafka topic to subscribe to
 * <keyword_1>			- The keyword to filter tweets
 * <keyword_n>			- Any number of keywords to filter tweets
 * 
 * More discussion at stdatalabs.blogspot.com
 * 
 * @author Sachin Thirumala
 */

public class KafkaTwitterProducer {
	public static void main(String[] args) throws Exception {
		final LinkedBlockingQueue<Status> queue = new LinkedBlockingQueue<Status>(1000);

		if (args.length < 5) {
			System.out.println(
					"Usage: KafkaTwitterProducer <twitter-consumer-key> <twitter-consumer-secret> <twitter-access-token> <twitter-access-token-secret> <broker-url> <topic-name> <twitter-search-keywords>");
			return;
		}

		String consumerKey = args[0].toString();
		String consumerSecret = args[1].toString();
		String accessToken = args[2].toString();
		String accessTokenSecret = args[3].toString();
		String brokerURL = args[4].toString();
		String topicName = args[5].toString();
		String[] arguments = args.clone();
		String[] keyWords = Arrays.copyOfRange(arguments, 6, arguments.length);
		

		
//		String consumerKey = "H8uzrzZGI8lzl9qZp4nl8791v";
//		String consumerSecret = "X6x9CCL4PM3DacQILyx3SQErYUcFbJGv3ghkGgU5cSXU0qYMGb";
//		String accessToken = "412468360-dyLD4IE5zDKkynOA8gSaHDhC6PkpCp9TbAIq8gZD";
//		String accessTokenSecret = "wdLst7o4eqGsukqOjyxcQcyIjRZEqc02P0TlcAODPr01j";
//		String topicName = "twitter";
//		String[] arguments = args.clone();
		//String[] keyWords = Arrays.copyOfRange(arguments, 5, arguments.length);
		
		

		// Set twitter oAuth tokens in the configuration
		ConfigurationBuilder cb = new ConfigurationBuilder();
		cb.setDebugEnabled(true).setOAuthConsumerKey(consumerKey).setOAuthConsumerSecret(consumerSecret)
				.setOAuthAccessToken(accessToken).setOAuthAccessTokenSecret(accessTokenSecret)
				.setHttpProxyHost("10.70.180.23").setHttpProxyPort(80);
		
		 
	    System.setProperty("http.proxyHost", "10.70.180.23");
	    System.setProperty("http.proxyPort", "80");
	    System.setProperty("https.proxyHost", "10.70.180.23");
	    System.setProperty("https.proxyPort", "80");
		
		

		// Create twitterstream using the configuration
		TwitterStream twitterStream = new TwitterStreamFactory(cb.build()).getInstance();
		StatusListener listener = new StatusListener() {

			
			public void onStatus(Status status) {
				queue.offer(status);
			}

			
			public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {
				System.out.println("Got a status deletion notice id:" + statusDeletionNotice.getStatusId());
			}

			
			public void onTrackLimitationNotice(int numberOfLimitedStatuses) {
				System.out.println("Got track limitation notice:" + numberOfLimitedStatuses);
			}

			
			public void onScrubGeo(long userId, long upToStatusId) {
				System.out.println("Got scrub_geo event userId:" + userId + "upToStatusId:" + upToStatusId);
			}

			
			public void onStallWarning(StallWarning warning) {
				System.out.println("Got stall warning:" + warning);
			}

			
			public void onException(Exception ex) {
				ex.printStackTrace();
			}
		};
		twitterStream.addListener(listener);

		// Filter keywords
		FilterQuery query = new FilterQuery().track("brasil").language("pt");
		twitterStream.filter(query);

		// Thread.sleep(5000);

		// Add Kafka producer config settings
		Properties props = new Properties();
		props.put("metadata.broker.list", "f321t018.prevnet:6667");
		props.put("bootstrap.servers", "f321t018.prevnet:6667");
		props.put("acks", "all");
		props.put("retries", 0);
		props.put("batch.size", 16384);
		props.put("linger.ms", 1);
		props.put("buffer.memory", 33554432);
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("security.protocol", "PLAINTEXTSASL");

		Producer<String, String> producer = new KafkaProducer<String, String>(props);
		int i = 0;
		int j = 0;

		// poll for new tweets in the queue. If new tweets are added, send them
		// to the topic
		while (true) {
			Status ret = queue.poll();

			if (ret == null) {
				Thread.sleep(100);
				// i++;
			} else {
				for (HashtagEntity hashtage : ret.getHashtagEntities()) {
					System.out.println("Tweet:" + ret);
					System.out.println("Hashtag: " + hashtage.getText());
					// producer.send(new ProducerRecord<String, String>(
					// topicName, Integer.toString(j++), hashtage.getText()));
					producer.send(new ProducerRecord<String, String>(topicName, Integer.toString(j++), ret.getText()));
				}
			}
		}
		// producer.close();
		// Thread.sleep(500);
		// twitterStream.shutdown();
	}

}
