package com.keedio.flink.integration

import com.datastax.driver.core.Cluster.Builder
import com.datastax.driver.core.{Cluster, ProtocolVersion, ResultSet}
import com.keedio.flink.EmbeddedCassandraServer
import org.apache.flink.api.java.tuple.Tuple2
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.cassandra.{CassandraSink, ClusterBuilder}
import org.hamcrest.MatcherAssert._
import org.hamcrest.Matchers._
import org.junit.{After, Assert, Test}

import scala.collection.JavaConverters._


/**
  * Created by luislazaro on 23/2/17.
  * lalazaro@keedio.com
  * Keedio
  */

class CassandraConnectorDirectQueryTest {
  val embeddedCassandraServer = new EmbeddedCassandraServer("redhatpoc.cql", "redhatpoc")
  val session = embeddedCassandraServer.getSession

  @After
  def after() = {
    embeddedCassandraServer.cleanupServer()
  }

  /**
    * Execution environment is Java
    */
  @Test
  def cassandraConnectorDirectQuerySinkTest = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val list: List[Tuple2[String, String]] = List(new Tuple2("4", "a"), new Tuple2("2", "b"), new Tuple2("3", "c"))
    val source: DataStream[Tuple2[String, String]] = env.fromCollection(list.asJava)

    CassandraSink.addSink(source)
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

    val result: ResultSet = session.execute("select * from redhatpoc.cassandraconnectorexample WHERE id='4';")
    assertThat(result.iterator.next.getString("text"), is("a"))
    val result1: ResultSet = embeddedCassandraServer.getSession.execute("select * from redhatpoc.cassandraconnectorexample WHERE id='3';")
    Assert.assertEquals(result1.iterator.next.getString("text"), "c")
  }
}


