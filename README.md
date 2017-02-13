## Flink consumer for Kafka
Flink app for processing event streams served by Apache Kafka

### Usage
´´´ /flink-1.2.0/bin/flink run -c com.keedio.flink.KafkaLogProcessor flink-kafka-consumer-0.0.1-SNAPSHOT.jar
 --topic test --bootstrap.servers 192.168.2.110:9092
 --zookeeper.connect 192.168.2.110:2181
 --group.id myGroup ´´´