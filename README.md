# kafka-consumer-to-opensearch
Studying kafka.

___

This is the piece of a simple architecture in Vanilla Java thats consume events from a Kafka topic and stores them in a Opensearch database

[Wikimedia Streaming Changes](https://stream.wikimedia.org/v2/stream/recentchange) -> [kafka-producer-wikimedia](https://github.com/roy-kafka/kafka-producer-wikimedia) -> [kafka-consumer-to-opensearch](this) -> [opensearchDB](https://opensearch.org/)


## Dependencies

- Kafka broker: localhost:9092
  - topic: 'wikimedia.recent.changes'
- Opensearch: localhost: 9092
  - index: 'wikimedia'
