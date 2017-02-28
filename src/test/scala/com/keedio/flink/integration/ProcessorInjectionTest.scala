package com.keedio.flink.integration

import java.sql.Timestamp

import com.datastax.driver.core.Cluster.Builder
import com.datastax.driver.core._
import com.datastax.driver.core.exceptions.DriverException
import com.keedio.flink.OpenStackLogProcessor.stringToTupleSS
import com.keedio.flink.{EmbeddedCassandraServer, OpenStackLogProcessor}
import org.apache.flink.api.java.tuple.{Tuple2, Tuple7}
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.runtime.client.JobExecutionException
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.cassandra.{CassandraSink, ClusterBuilder}
import org.hamcrest.MatcherAssert._
import org.hamcrest.Matchers._
import org.junit.{Test, _}

import scala.collection.Map

/**
  * Created by luislazaro on 24/2/17.
  * lalazaro@keedio.com
  * Keedio
  */
class ProcessorInjectionTest {
  val embeddedCassandraServer = new EmbeddedCassandraServer("redhatpoc.cql", "redhatpoc")
  val session = embeddedCassandraServer.getSession

  @After
  private[integration] def after() = {
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
    *
    * Test: create and populate stack_services
    */
  @Test(timeout = 60000)
  def inyectStackServicesData() = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val stream: DataStream[String] = env.readTextFile("./src/test/resources/example.log")
    stream.rebalance.print

    val listOfKeys: Map[String, Int] = Map("1h" -> 3600, "6h" -> 21600, "12h" -> 43200, "24h" -> 86400, "1w" ->
      604800, "1m" -> 2419200)

    val listStackService: Iterable[DataStream[Tuple7[String, String, String, String, Int,String, Int]]] = listOfKeys
      .map(e => stringToTupleSS(stream, e._1, e._2, "boston"))

    listStackService.foreach { t =>  {
          CassandraSink.addSink(t.javaStream)
            .setQuery("INSERT INTO redhatpoc.stack_services (id, region, loglevel, service, ts, timeframe, tfhours) VALUES (?," +
              "?," +
              "?,?, now(),?,?) USING TTL " + "?" + ";")
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
    }


    env.execute()

    val full = session.execute("select id, service, loglevel, region, dateOf(ts), timeframe, tfhours, TTL(timeframe) from " +
      "redhatpoc.stack_services")
    val a: Array[AnyRef] = full.all().toArray()
    a.foreach(println)
          val result: ResultSet = session.execute("select * from redhatpoc.stack_services WHERE id='24h';")
          assertThat(result.iterator.next.getString("id"), is("24h"))
          assertThat(result.iterator.next.getString("region"), is("boston"))
          assertThat(result.iterator.next.getString("loglevel"), isOneOf("WARNING", "INFO", "ERROR"))
  }

  /**
    * * Test: create and populate raw_logs
    */
  @Test(timeout = 60000)
  def inyectRawLogData() = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val stream: DataStream[String] = env.readTextFile("./src/test/resources/example.log")
    stream.rebalance.print
    val rawLog: DataStream[Tuple7[String, String, String, String, String, Timestamp, String]] = OpenStackLogProcessor
      .stringToTupleRL(stream, "boston")

    CassandraSink.addSink(rawLog.javaStream)
      .setQuery("INSERT INTO redhatpoc.raw_logs (date, region, loglevel, service, node_type, log_ts, payload) VALUES " +
        "(?, ?, " +
        "?, ?, ?, ?, ?);")
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

    env.execute()
    val full = session.execute("select * from redhatpoc.raw_logs LIMIT 10")
    val a: Array[AnyRef] = full.all().toArray()
    a.foreach(println)
  }

  @Test
  def should_launch_exception_for_invalidQueryTest() = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val list: List[Tuple2[String, String]] = List(new Tuple2("4", "a"), new Tuple2("2", "b"), new Tuple2("3", "c"))
    val source = env.fromCollection(list)

  CassandraSink.addSink(source.javaStream)
    .setQuery("INSERT INTO redhatpoc.cassandraconnectorexample (id, NO_EXISTE_ESTA_COLUMNA) VALUES (?, ?) USING TTL 20 ")

    .setClusterBuilder(new ClusterBuilder() {
      override def buildCluster(builder: Builder): Cluster = builder
        .addContactPoint("127.0.0.1")
        .withPort(9142)
        .withProtocolVersion(ProtocolVersion.V3)
        .build()
    })
    .build()

    try {
  env.execute()

    } catch {
    case e : DriverException => println("Error: invalid query")
    case e : JobExecutionException => println("Error: flink JobExecution")
}

    println

  }


}
