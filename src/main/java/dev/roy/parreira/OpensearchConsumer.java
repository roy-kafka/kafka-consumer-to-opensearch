package dev.roy.parreira;

import com.google.gson.JsonParser;
import com.google.gson.JsonSyntaxException;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.DefaultConnectionKeepAliveStrategy;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.client.indices.CreateIndexRequest;
import org.opensearch.client.indices.GetIndexRequest;
import org.opensearch.common.xcontent.XContentType;

import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.util.List;
import java.util.Properties;

import static org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;
import static org.opensearch.client.RequestOptions.DEFAULT;

@Slf4j
public class OpensearchConsumer {

  private static final String WIKIMEDIA_INDEX = "wikimedia";
  private static final String WIKIMEDIA_RECENT_CHANGES_TOPIC = "wikimedia.recent.changes";

  public static RestHighLevelClient createOpensearchClient() {
    RestHighLevelClient restHighLevelClient;
    URI openSearchConnectionUri = URI.create("http://localhost:9200");

    String userInfo = openSearchConnectionUri.getUserInfo();

    if (userInfo == null) {

      // REST client without security
      restHighLevelClient = new RestHighLevelClient(
          RestClient.builder(
              new HttpHost(
                  openSearchConnectionUri.getHost(),
                  openSearchConnectionUri.getPort(),
                  "http"
              )
          )
      );

    } else {

      // REST client with security
      String[] auth = userInfo.split(":");

      CredentialsProvider cp = new BasicCredentialsProvider();
      cp.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(auth[0], auth[1]));

      restHighLevelClient = new RestHighLevelClient(
          RestClient.builder(
              new HttpHost(
                  openSearchConnectionUri.getHost(),
                  openSearchConnectionUri.getPort(),
                  openSearchConnectionUri.getScheme()
              )
          ).setHttpClientConfigCallback(
              httpAsyncClientBuilder ->
                  httpAsyncClientBuilder
                      .setDefaultCredentialsProvider(cp)
                      .setKeepAliveStrategy(new DefaultConnectionKeepAliveStrategy())
          )
      );
    }

    return restHighLevelClient;
  }

  private static KafkaConsumer<String, String> createKafkaConsumer() {

    String boostrapServers = "127.0.0.1:9092";
    String groupId = "consumer-opensearch";

    Properties properties = new Properties();
    properties.setProperty(BOOTSTRAP_SERVERS_CONFIG, boostrapServers);
    properties.setProperty(KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    properties.setProperty(VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    properties.setProperty(GROUP_ID_CONFIG, groupId);
    properties.setProperty(AUTO_OFFSET_RESET_CONFIG, "earliest");
    properties.setProperty(ENABLE_AUTO_COMMIT_CONFIG, "false");

    KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
    consumer.subscribe(List.of(WIKIMEDIA_RECENT_CHANGES_TOPIC));

    return consumer;
  }

  private static void createWikimediaIndexIfNotExists(RestHighLevelClient openSearchClient) throws IOException {

    if (openSearchClient.indices().exists(new GetIndexRequest(WIKIMEDIA_INDEX), DEFAULT)) {
      log.info("The index: '{}' already exists.", WIKIMEDIA_INDEX);
      return;
    }

    openSearchClient.indices().create(new CreateIndexRequest(WIKIMEDIA_INDEX), DEFAULT);
    log.info("Index {} created!!!", WIKIMEDIA_INDEX);
  }

  private static String extractId(String json) {
    String extractedId;

    try {
      extractedId = JsonParser.parseString(json)
          .getAsJsonObject()
          .get("meta")
          .getAsJsonObject()
          .get("id")
          .getAsString();
    } catch (JsonSyntaxException e) {
      log.warn("Json message may not have the property 'id' in it!");
      extractedId = "";
    }

    return extractedId;
  }

  private static void consumeFromKafkaStoreOnOpenSearch(
      RestHighLevelClient openSearchClient,
      KafkaConsumer<String, String> consumer
  ) throws IOException, InterruptedException {

    while (true) { // This is only for studying, NOT PRODUCTION CODE
      ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofMillis(3000));
      log.info("Received " + consumerRecords.count() + " record(s)");

      BulkRequest bulkRequest = new BulkRequest();

      for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {

        String id = extractId(consumerRecord.value());
        if (id.isEmpty()) continue;

        IndexRequest indexRequest = new IndexRequest(WIKIMEDIA_INDEX)
            .source(consumerRecord.value(), XContentType.JSON)
            .id(id);

        bulkRequest.add(indexRequest);
      }


      if (bulkRequest.numberOfActions() > 0) {
        BulkResponse bulkResponse = openSearchClient.bulk(bulkRequest, RequestOptions.DEFAULT);
        log.info("Inserted " + bulkResponse.getItems().length + " record(s).");

        Thread.sleep(1000); // This is only for studying, NOT PRODUCTION CODE

        consumer.commitSync();
        log.info("Offsets have been committed!");
      }
    }
  }


  public static void main(String[] args) throws IOException, InterruptedException {
    RestHighLevelClient openSearchClient = createOpensearchClient();
    KafkaConsumer<String, String> consumer = createKafkaConsumer();

    try (openSearchClient; consumer) {
      createWikimediaIndexIfNotExists(openSearchClient);
      consumeFromKafkaStoreOnOpenSearch(openSearchClient, consumer);
    }
  }
}
