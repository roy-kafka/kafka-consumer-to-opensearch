# kafka-consumer-to-opensearch
Studying kafka.

___

This is the piece of a simple architecture in Vanilla Java thats consume events from a Kafka topic and stores them in a Opensearch database

[Wikimedia Streaming Changes](https://stream.wikimedia.org/v2/stream/recentchange) -> [kafka-producer-wikimedia](https://github.com/roy-kafka/kafka-producer-wikimedia) -> [<ins>***kafka-consumer-to-opensearch***</ins>](https://github.com/roy-kafka/kafka-consumer-to-opensearch) -> [opensearchDB](https://opensearch.org/)


## Dependencies

- Kafka broker: localhost:9092
  - topic: 'wikimedia.recent.changes'
- Opensearch: localhost: 9092
  - index: 'wikimedia'
