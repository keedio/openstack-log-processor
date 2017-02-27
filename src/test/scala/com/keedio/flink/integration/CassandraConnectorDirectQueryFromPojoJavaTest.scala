package com.keedio.flink.integration

import com.datastax.driver.core.Cluster.Builder
import com.datastax.driver.core.{Cluster, ProtocolVersion}
import com.keedio.flink.EmbeddedCassandraServer
import com.keedio.flink.dbmodels.PojoSampleJava
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.cassandra.{CassandraSink, ClusterBuilder}
import org.junit.{After, Test}

/**
  * Created by luislazaro on 23/2/17.
  * lalazaro@keedio.com
  * Keedio
  */
class CassandraConnectorDirectQueryFromPojoJavaTest {
  val embeddedCassandraServer = new EmbeddedCassandraServer("redhatpoc.cql", "redhatpoc")
  val session = embeddedCassandraServer.getSession

  @After
  def after() = {
    embeddedCassandraServer.cleanupServer()
  }

  /**
    * Must launch java.lang.IllegalArgumentException
    * Specifying a query is not allowed when using a Pojo-Stream as input
    */
  @Test(expected = classOf[java.lang.IllegalArgumentException])
  def connectorCassandarPojoToSinkTest() = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val list: List[PojoSampleJava] = List(new PojoSampleJava("1", "a"), new PojoSampleJava("2", "b"), new PojoSampleJava("3", "c"))
    val source: DataStream[PojoSampleJava] = env.fromCollection(list)

    CassandraSink.addSink(source.javaStream)
      .setQuery("INSERT INTO redhatpoc.cassandraconnectorexample (id, text) VALUES (?, ?) USING TTL 20 ")
      .setClusterBuilder(new ClusterBuilder() {
        override def buildCluster(builder: Builder): Cluster = builder
          .addContactPoint("127.0.0.1")
          .withPort(9142)
          .withProtocolVersion(ProtocolVersion.V3)
          .build()
      })
      .build()

    env.execute()
    println
  }
}
