package com.keedio.flink.integration

import com.datastax.driver.core.Cluster.Builder
import com.datastax.driver.core._
import com.keedio.flink.{EmbeddedCassandraServer, OpenStackLogProcessor}
import org.apache.flink.api.java.tuple.Tuple5
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.cassandra.{CassandraSink, ClusterBuilder}
import org.hamcrest.MatcherAssert._
import org.hamcrest.Matchers._
import org.junit.{After, Test}

import scala.collection.Map

/**
  * Created by luislazaro on 24/2/17.
  * lalazaro@keedio.com
  * Keedio
  */
class SimpleProcessorTest {
  val embeddedCassandraServer = new EmbeddedCassandraServer("rh.cql", "rh")
  val session = embeddedCassandraServer.getSession

  @After
  def after() = {
    embeddedCassandraServer.cleanupServer()
  }

  /**
    * According example.log file:
    * 1> (24h,boston,ERROR,Glance,801)
    * 1> (24h,boston,WARNING,Cinder,407)
    * 1> (6h,boston,WARNING,Glance,407)
    * 1> (1w,boston,ERROR,Pacemaker,801)
    * 1> (1m,boston,ERROR,Cinder,801)
    * 1> (1w,boston,WARNING,Cinder,407)
    * 1> (12h,boston,WARNING,Pacemaker,407)
    * 1> (1w,boston,INFO,Nova,1102)
    * 1> (1m,boston,WARNING,Keystone,407)
    * 1> (24h,boston,INFO,Glance,1102)
    * 1> (1m,boston,INFO,Storage,1102)
    */
  @Test
  def inyectStackServicesData() = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val stream: DataStream[String] = env.readTextFile("./src/test/resources/example.log")
    stream.rebalance.print

    val listOfKeys: Map[String, Int] = Map("1h" -> 3600, "6h" -> 21600, "12h" -> 43200, "24h" -> 86400, "1w" -> 604800, "1m" -> 2419200)

    val listStackService: Map[DataStream[Tuple5[String, String, String, String, Int]], Int] = listOfKeys
      .map(e => (OpenStackLogProcessor.stringToTupleSS(stream, e._1, e._2, "boston"), e._2))

    listStackService.map(t => t._1.rebalance.print)

    listStackService.foreach { t => CassandraSink.addSink(t._1.javaStream)
      .setQuery("INSERT INTO rh.stack_services (id, region, loglevel, service, ts, timeframe) VALUES (?, ?, ?, ?, now(), ?) USING TTL " + t._2 + ";")
      .setClusterBuilder(new ClusterBuilder() {
        override def buildCluster(builder: Builder): Cluster = {
          builder
            .addContactPoint("127.0.0.1")
            .withPort(9142)
            .withProtocolVersion(ProtocolVersion.V3)
            .build()
        }
      })
      .build()
    }
    env.execute()

    val result: ResultSet = session.execute("select * from rh.stack_services WHERE id='24h';")
    assertThat(result.iterator.next.getString("id"), is("24h"))
    assertThat(result.iterator.next.getString("region"), is("boston"))
    assertThat(result.iterator.next.getString("loglevel"), isOneOf("WARNING","INFO","ERROR"))

//    val full = session.execute("select * from rh.stack_services LIMIT 10")
//    val a: Array[AnyRef] = full.all().toArray()
//    a.foreach(println)


  }

}
