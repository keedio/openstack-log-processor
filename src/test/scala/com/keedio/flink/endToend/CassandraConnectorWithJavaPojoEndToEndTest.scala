package com.keedio.flink.endToend

import com.datastax.driver.core.Cluster
import com.datastax.driver.core.Cluster.Builder
import com.keedio.flink.PojoSampleJava
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.cassandra.{CassandraSink, ClusterBuilder}

/**
  * Created by luislazaro on 9/2/17.
  * lalazaro@keedio.com
  * Keedio
  */

/**
  * If a cassandra db exists in localhost, three rows will be inserted:
  *  id | text
  * ----+------
  *  3 |    c
  *  2 |    b
  *  1 |    a
  *  (3 rows)
  *
  *  Set in cassandra.yaml:
  *  rpc_address: 0.0.0.0
  *  broadcast_rpc_address: 127.0.0.1
  *  cassandra's shell:
  *  create keyspace IF NOT EXISTS redhatpoc with replication = {'class': 'SimpleStrategy', 'replication_factor': 1};
  *  CREATE TABLE redhatpoc.cassandraconnectorexample (id varchar, text varchar);
  * **/
object CassandraConnectorWithJavaPojoEndToEndTest {
  def main(args: Array[String]) {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val list = List(new PojoSampleJava("1", "a"), new PojoSampleJava("2", "b"), new PojoSampleJava("3", "c"))
    val source: DataStream[PojoSampleJava] = env.fromCollection(list)

    CassandraSink.addSink(source.javaStream)
        .setClusterBuilder(new ClusterBuilder() {
        override def buildCluster(builder: Builder): Cluster = {
          builder.addContactPoint("192.168.2.110")
            .build()
        }
      })
      .build()
    env.execute()
  }
}
