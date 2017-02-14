package com.keedio.flink

import com.datastax.driver.core.Cluster
import com.datastax.driver.core.Cluster.Builder
import collection.JavaConverters._
import org.apache.flink.streaming.connectors.cassandra.{CassandraSink, ClusterBuilder}
import org.apache.flink.api.java.tuple.Tuple2
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment

/**
  * Created by luislazaro on 13/2/17.
  * lalazaro@keedio.com
  * Keedio
  */
object CassandarConnectorEndToEndTest2 {
  def main(args: Array[String]) {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val list: List[Tuple2[String, String]] = List(new Tuple2("4", "a"), new Tuple2("2", "b"), new Tuple2("3", "c"))
    val source: DataStream[Tuple2[String, String]] = env.fromCollection(list.asJava)

    CassandraSink.addSink(source)
        .setQuery("INSERT INTO redhatpoc.cassandraconnectorexample (id, text) VALUES (?, ?) USING TTL 20 ")
      .setClusterBuilder(new ClusterBuilder() {
        override def buildCluster(builder: Builder): Cluster = {
          builder.addContactPoint("192.168.2.110").build()
        }
      })
      .build()
    env.execute()
  }
}
