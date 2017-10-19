package br.gov.dataprev.twitter.kafta;

import java.io.File;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import com.google.common.collect.Lists;
import com.twitter.bijection.Injection;
import com.twitter.bijection.avro.GenericAvroCodecs;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.BasicClient;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import com.twitter.hbc.twitter4j.Twitter4jStatusClient;
import com.twitter.hbc.twitter4j.handler.StatusStreamHandler;
import com.twitter.hbc.twitter4j.message.DisconnectMessage;
import com.twitter.hbc.twitter4j.message.StallWarningMessage;

import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.User;


/**
 * Example Kafka Twitter Producer
 *
 * Wrap a Twitter4j Client around a Hosebird Client using a custom Status Listener
 * to connect to the Twitter Streaming API. Parse and convert the messages (tweets)
 * to Avro format and publish them to a Kafka Topic.
 *
 * Usage: TwitterProducer
 *  broker: The hostname and port at which the Kafka Broker is listening
 *
 * @author jillur.quddus
 * @version 0.0.1
 *
 */

public class TwitterProducer {

    // Kafka Producer - note that Kafka Producers are Thread Safe and that sharing a Producer instance
    // across threads is generally faster than having multiple Producer instances
    private static KafkaProducer<String, byte[]> producer;

    // Note that for a Production Deployment, do not hard-code your Twitter Application Authentication Keys
    // Instead, derive from a Configuration File or Context
//    private static final String CONSUMER_KEY = "H8uzrzZGI8lzl9qZp4nl8791v";
//    private static final String CONSUMER_SECRET = "X6x9CCL4PM3DacQILyx3SQErYUcFbJGv3ghkGgU5cSXU0qYMGb";
//    private static final String ACCESS_TOKEN = "412468360-dyLD4IE5zDKkynOA8gSaHDhC6PkpCp9TbAIq8gZD";
//    private static final String ACCESS_TOKEN_SECRET = "wdLst7o4eqGsukqOjyxcQcyIjRZEqc02P0TlcAODPr01j";
      private  static final String KAFKA_TOPIC = "twitter";
    
    
    

    // Avro Schema to use to serialise messages to the Kafka Topic
    // For the full list of Tweet fields, please refer to
    // https://dev.twitter.com/overview/api/tweets
    private static Schema schema;
    private static Injection<GenericRecord, byte[]> recordInjection;
    public static final String TWEET_SCHEMA = "{"
            + "\"type\":\"record\","
            + "\"name\":\"tweet\","
            + "\"fields\":["
            + "  { \"name\":\"id\", \"type\":\"long\" },"
            + "  { \"name\":\"created_at\", \"type\":\"string\" },"
            + "  { \"name\":\"source\", \"type\":\"string\" },"
            + "  { \"name\":\"coordenada_latitude\", \"type\":\"double\" },"
            + "  { \"name\":\"coordenada_longitude\", \"type\":\"double\" },"
            + "  { \"name\":\"text\", \"type\":\"string\" },"
            + "  { \"name\":\"user_id\", \"type\":\"long\" },"
            + "  { \"name\":\"user_name\", \"type\":\"string\" },"
            + "  { \"name\":\"user_location\", \"type\":\"string\" },"
            + "  { \"name\":\"user_url\", \"type\":\"string\" },"
            + "  { \"name\":\"user_description\", \"type\":\"string\" },"
            + "  { \"name\":\"user_verified\", \"type\":\"string\" },"
            + "  { \"name\":\"user_followers_count\", \"type\":\"int\" },"
            + "  { \"name\":\"user_friends_count\", \"type\":\"int\" },"
            + "  { \"name\":\"user_listed_count\", \"type\":\"int\" },"
            + "  { \"name\":\"user_favourites_count\", \"type\":\"int\" },"
            + "  { \"name\":\"user_statuses_count\", \"type\":\"int\" },"
            + "  { \"name\":\"user_created_at\", \"type\":\"string\" },"
            + "  { \"name\":\"user_utc_offset\", \"type\":\"int\" },"
            + "  { \"name\":\"user_time_zone\", \"type\":\"string\" },"
            + "  { \"name\":\"user_geo_enabled\", \"type\":\"string\" },"
            + "  { \"name\":\"user_lang\", \"type\":\"string\" },"
            + "  { \"name\":\"user_profile_image_url_https\", \"type\":\"string\" },"           
            + "  { \"name\":\"user_screen_name\", \"type\":\"string\" }"            
            + "]}";
    
    
    
    
    
    

    /**
     * Wrap a Twitter4j Client around a Hosebird Client using a custom Status Listener
     * and an Executor Service to spawn threads to parse the messages received
     * @param kafkaBroker
     * @throws InterruptedException
     */

    public static void run(String kafkaBroker,String kafkaTopic,String consumerKey, String consumerSecret,String accessToken, String accessTokenSecret, String proxyHost, Integer proxyPort, String[] keyWords ) throws InterruptedException {

   	
    	    	
        // Kafka Producer Properties
        Properties producerProperties = new Properties();

        // Bootstrapping
        producerProperties.put("bootstrap.servers", kafkaBroker);

        // Serializer Class for Keys
        producerProperties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        // Serializer Class for Values
        producerProperties.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");

        // When a Produce Request is considered completed
        producerProperties.put("request.required.acks", "1");

        // Create the Kafka Producer
        producer = new KafkaProducer(producerProperties);

        // Twitter Connection and Filtering Properties
        BlockingQueue<String> messageQueue = new LinkedBlockingQueue<String>(100000);
        StatusesFilterEndpoint endpoint = new StatusesFilterEndpoint();
        //endpoint.languages(Lists.newArrayList("pt"));
        endpoint.stallWarnings(false);
        endpoint.followings(Lists.newArrayList(new Long(412468360)));
        //endpoint.trackTerms(Lists.newArrayList(keyWords));
        Authentication authentication = new OAuth1(consumerKey, consumerSecret, accessToken, accessTokenSecret);

        // Build a Twitter Hosebird Client
        ClientBuilder hosebirdClientBuilder = new ClientBuilder()
                .name("Keisan Knowledgebase Twitter Hosebird Client")
                .hosts(Constants.STREAM_HOST)
                .authentication(authentication)
                .endpoint(endpoint) 
                .processor(new StringDelimitedProcessor(messageQueue));
        
        if(!proxyHost.isEmpty())
        	hosebirdClientBuilder.proxy(proxyHost, proxyPort);
        
        
        BasicClient hosebirdClient = hosebirdClientBuilder.build();

        // Create an Executor Service to spawn threads to parse the messages
        // Runnables are submitted to the Executor Service to process the Message Queue
        int numberProcessingThreads = 20;
        ExecutorService service = Executors.newFixedThreadPool(numberProcessingThreads);

        // Wrap a Twitter4j Client around the Hosebird Client using a custom Status Listener
        Twitter4jStatusClient twitter4jClient = new Twitter4jStatusClient(
                hosebirdClient, messageQueue, Lists.newArrayList(statusListener), service);

        // Connect to the Twitter Streaming API
        twitter4jClient.connect();

        // Twitter4jStatusClient.process must be called for every Message Processing Thread to be spawned
        for (int threads = 0; threads < numberProcessingThreads; threads++) {
            twitter4jClient.process();
        }

        // Run the Producer for 60 seconds for DEV purposes
        // Note that this is NOT a graceful exit
        //Thread.sleep(1000);
        //producer.close();
        //hosebirdClient.stop();

    }

    /**
     * Custom Status Listener
     * The onStatus method gets called for every new Tweet. It is here where we
     * will parse the incoming messages and generate the Avro Record which will be
     * serialised and sent using our Kafka Producer.
     */

    private static StatusListener statusListener = new StatusStreamHandler() {

        public void onStatus(Status status) {

            // Convert the Status object into an Avro Record for serialising and publishing to the Kafka Topic
            GenericData.Record avroRecord = createRecord(status);
            byte[] avroRecordBytes = recordInjection.apply(avroRecord);
            ProducerRecord<String, byte[]> record = new ProducerRecord(KAFKA_TOPIC, avroRecordBytes);
            // Send the Message to Kafka
            producer.send(record);

        }

        public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {}

        public void onTrackLimitationNotice(int limit) {}

        public void onScrubGeo(long user, long upToStatus) {}

        public void onStallWarning(StallWarning warning) {}

        public void onException(Exception e) {}

        public void onDisconnectMessage(DisconnectMessage message) {}

        public void onStallWarningMessage(StallWarningMessage warning) {}


        public void onUnknownMessageType(String s) {}

    };

    /**
     * Parse and convert the Tweet Status into an Avro Record for serialising
     * and publishing to the Kafka Topic.
     * @param avroSchema
     * @param status
     * @return
     */

    private static GenericData.Record createRecord(Status status) {

        User user = status.getUser();
        GenericData.Record doc = new GenericData.Record(schema);
        doc.put("id", status.getId());
        doc.put("user_name", user.getName());
        doc.put("user_id", user.getId());
        doc.put("user_screen_name", user.getScreenName());
        doc.put("user_location", user.getLocation());
        doc.put("user_url", user.getURL());
        doc.put("user_description", user.getDescription());
        doc.put("user_followers_count", user.getFollowersCount());
        doc.put("user_friends_count", user.getFriendsCount());
        doc.put("user_listed_count", user.getListedCount());
        doc.put("user_favourites_count", user.getFavouritesCount());
        doc.put("user_statuses_count", user.getStatusesCount());
        doc.put("user_created_at", user.getCreatedAt().toString());
        doc.put("user_utc_offset", user.getUtcOffset());
        doc.put("user_time_zone", user.getTimeZone());
        doc.put("user_lang", user.getLang());
        doc.put("user_profile_image_url_https", user.getProfileImageURL());
        doc.put("text", status.getText());
        doc.put("created_at", status.getCreatedAt().toString());
        doc.put("source", status.getSource());
        if(status.getGeoLocation()!=null){
        	 doc.put("coordenada_latitude", status.getGeoLocation().getLatitude());
             doc.put("coordenada_longitude", status.getGeoLocation().getLongitude());
        }
        
//        + "  { \"name\":\"user_verified\", \"type\":\"string\" },"
//        + "  { \"name\":\"user_geo_enabled\", \"type\":\"string\" },"
        
        
        
        
        System.out.println(doc.toString());
        return doc;

    }

    public static void main(String[] args) {

    	
//    	if (args.length < 9) {
//			System.out.println(
//					"Usage: TwitterProducer <brokerURL> <topicName> <twitter-consumer-key> <twitter-consumer-secret> <twitter-access-token> <twitter-access-token-secret> <proxy-host> <proxy-port> <twitter-search-keywords>");
//			System.exit(1);
//		}
    	
//    	String brokerURL = args[0].toString();
//    	String topicName = args[1].toString();
//		String consumerKey = args[2].toString();
//		String consumerSecret = args[3].toString();
//		String accessToken = args[4].toString();
//		String accessTokenSecret = args[5].toString();
//		String proxyHost = args[6].toString();
//		Integer proxyPort = Integer.valueOf(args[7].toString());
//		String[] arguments = args.clone();
//		String[] keyWords = Arrays.copyOfRange(arguments, 8, arguments.length);
		
		
		String brokerURL = "f321t018.prevnet:6667";
    	String topicName = "twitter";
		String consumerKey = "H8uzrzZGI8lzl9qZp4nl8791v";
		String consumerSecret = "X6x9CCL4PM3DacQILyx3SQErYUcFbJGv3ghkGgU5cSXU0qYMGb";
		String accessToken = "412468360-dyLD4IE5zDKkynOA8gSaHDhC6PkpCp9TbAIq8gZD";
		String accessTokenSecret = "wdLst7o4eqGsukqOjyxcQcyIjRZEqc02P0TlcAODPr01j";
		String proxyHost = "10.70.180.23";
		Integer proxyPort = 80;
		String[] keyWords = {"brasil","dataprev"};
		
		
        try {

            Schema.Parser parser = new Schema.Parser();
            schema = parser.parse(TWEET_SCHEMA);
            recordInjection = GenericAvroCodecs.toBinary(schema);

            // Connect to the Twitter Streaming API and start the Producer
            TwitterProducer.run(brokerURL,topicName,consumerKey,consumerSecret,accessToken,accessTokenSecret,proxyHost,proxyPort,keyWords);

        } catch (Exception e) {

            System.out.println(e);

        }

    }

}