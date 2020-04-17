package com.github.andraspapaz.consumer;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ElasticSearchConsumer {

    private static Logger LOGGER = LoggerFactory.getLogger(ElasticSearchConsumer.class.getName());
    static Gson g = new Gson();

    public static RestHighLevelClient createClient() {
        String hostname = "kafkatwitterdemo-9997063964.us-east-1.bonsaisearch.net";
        String username = "rtt6ygzt5l";
        String password = "7jzif4nmv3";

        // don't do if you run a local ES
        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(username, password));

        RestClientBuilder builder = RestClient.builder(new HttpHost(hostname, 443, "https"))
        .setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
            @Override
            public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {
                return httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
            }
        });

        RestHighLevelClient client = new RestHighLevelClient(builder);
        return client;
    }

    public static void main(String[] args) throws IOException {
        JsonParser parser = new JsonParser();

        RestHighLevelClient client = createClient();
        KafkaConsumer<String, String> consumer = createConsumer("twitter_topic");

        // poll for new data
        while(true) {
            ConsumerRecords<String, String> records = consumer.poll((Duration.ofMillis(100)));

            Integer recordCount = records.count();
            LOGGER.info("Received " + recordCount + " records");
            BulkRequest bulkRequest = new BulkRequest();

            for (ConsumerRecord record : records) {
                // normalize data, twitter is not friendly and needs additional work
                JsonElement jsonObject = parser.parse(record.value().toString());

                // The old Indexrequest would require (type & id) the id was used to help with Idempodent
                IndexRequest indexRequest = new IndexRequest("twitter").source(jsonObject.toString(), XContentType.JSON);

                // For BulkRequests
                bulkRequest.add(indexRequest);

                // Single Request
                // IndexResponse indexResponse = client.index(indexRequest, RequestOptions.DEFAULT);
                // String id = indexResponse.getId();
                // LOGGER.info(id);

                // THIS IS NOT NEEDED FOR BULKREQUEST
//                try {
//                    Thread.sleep(10);
//                } catch (InterruptedException e) {
//                    e.printStackTrace();
//                }
            }

            if (recordCount > 0) {
                BulkResponse bulkItemResponses = client.bulk(bulkRequest, RequestOptions.DEFAULT);
                LOGGER.info("Comminting Offset ....");
                consumer.commitSync();
                LOGGER.info("Offsets have been commited");
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }

        // close client gracefully
//        client.close();
    }

    public static KafkaConsumer<String, String> createConsumer(String topic) {
        String bootStrapServer = "127.0.0.1:9092";
        String consumerGroupId = "twitter_topic_group_elastic_search";
        String resetConfig = "earliest";

        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServer);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, consumerGroupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, resetConfig);
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        // number of records at a time
        properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "100");

        // create consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        // Subscribe to topic(s)
        consumer.subscribe(Arrays.asList(topic));

        return consumer;
    }
}
