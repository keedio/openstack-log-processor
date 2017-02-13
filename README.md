## OpenStack Log Processor
Flink app for processing event streams served by Apache Kafka

### Usage
./flink-1.2.0/bin/flink run -c com.keedio.flink.OpenStackLogProcessor openStack-log-processor-0.0.1-SNAPSHOT.jar \
 --topic test
 --bootstrap.servers localhost:9092 \
 --zookeeper.connect localhost:2181 \
 --group.id myGroup \
 --cassandra.host localhost