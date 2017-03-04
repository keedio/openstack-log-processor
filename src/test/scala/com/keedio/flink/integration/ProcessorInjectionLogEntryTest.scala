package com.keedio.flink.integration

import java.sql.Timestamp

import com.datastax.driver.core.Cluster.Builder
import com.datastax.driver.core._
import com.keedio.flink.EmbeddedCassandraServer
import com.keedio.flink.OpenStackLogProcessor._
import com.keedio.flink.entities.LogEntry
import com.keedio.flink.utils.ProcessorHelper
import org.apache.flink.api.java.tuple.{Tuple5, Tuple7}
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.cassandra.{CassandraSink, ClusterBuilder}
import org.hamcrest.MatcherAssert._
import org.hamcrest.Matchers._
import org.hamcrest.number.OrderingComparisons.lessThanOrEqualTo
import org.junit._

import scala.collection.JavaConversions._
import scala.collection.Map
import scala.collection.immutable.Seq

/**
  * Created by luislazaro on 1/3/17.
  * lalazaro@keedio.com
  * Keedio
  */

/**
  * 1) Stand up an embedded cassandra server
  * 2) create environment for flink
  * 3) stream of string to entity LogEntity
  * 4) transform stream of logentity to tuple
  * 5) inyect into tuple(table) into cassandra - each junit test is a table.
  * 6) query data and validate.
  */
class ProcessorInjectionLogEntryTest {
  val embeddedCassandraServer = new EmbeddedCassandraServer("redhatpoc.cql", "redhatpoc")
  val session = embeddedCassandraServer.getSession
  val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
  val listOfKeys: Map[String, Int] = Map("1h" -> 3600, "6h" -> 21600, "12h" -> 43200, "24h" -> 86400, "1w" ->
    604800, "1m" -> 2419200)
  val listOfTimestamps: Seq[String] = ProcessorHelper.generateTimestamps()
  val listOfDummyLogs: Seq[String] = ProcessorHelper.generateListOflogs(listOfTimestamps)
  val stream: DataStream[String] = env.fromCollection(listOfDummyLogs)
  //parse json as LogEntry
  val streamOfLogs: DataStream[LogEntry] = stream.map(string => LogEntry(string))

  @After
  private[integration] def after() = {
    embeddedCassandraServer.cleanupServer()
  }

  /**
    * test: create and populate stack_services.
    */
  @Test(timeout = 60000)
  def inyectLogEntryStackServicesData() = {
    val listStackService: Iterable[DataStream[Tuple7[String, String, String, String, Int, String, Int]]] = listOfKeys
      .map(e => logEntryToTupleSS(streamOfLogs, e._1, e._2, "boston"))


    listStackService.foreach(t => CassandraSink.addSink(t.javaStream).setQuery(
      "INSERT INTO redhatpoc.stack_services (id, region, loglevel, service, ts, timeframe, tfhours) " +
        "VALUES (?,?,?,?, now(),?,?) USING TTL " + "?" + ";").setClusterBuilder(new ClusterBuilder() {
      override def buildCluster(builder: Builder): Cluster = {
        builder
          .addContactPoint("127.0.0.1")
          .withPort(9142)
          .withProtocolVersion(ProtocolVersion.V3)
          .build()
      }
    })
      .build())

    env.execute()

    val full = session.execute("select id, service, loglevel, region, dateOf(ts), timeframe, tfhours, TTL(timeframe) " +
      "from redhatpoc.stack_services")
    val a: Array[AnyRef] = full.all().toArray()
    a.foreach(println)
    val result: ResultSet = session.execute("select * from redhatpoc.stack_services WHERE id='24h';")
    assertThat(result.iterator.next.getString("id"), is("24h"))
    assertThat(result.iterator.next.getString("region"), is("boston"))
    assertThat(result.iterator.next.getString("loglevel"), isOneOf("WARNING", "ERROR", "INFO"))
    assertThat(result.iterator.next.getString("loglevel"), not(isOneOf("emerg", "aler", "crit", "notice", "debug")))

    //validate ttl's value
    //Assert that computed TTL is always less than or equal the corresponding value of temporal key.
    //example: if valkey is 3600 seconds, computed TTL cannot be bigger than former.
    listOfKeys.foreach(kv => {
      val result = session.execute("select TTL(timeframe) from redhatpoc.stack_services WHERE id=" + "'" + kv._1 + "';")
      val allresultsByKey: Seq[Row] = result.all().toIndexedSeq
      allresultsByKey.foreach(row => assertThat(Integer.valueOf(row.getInt(0)), lessThanOrEqualTo(Integer.valueOf
      (kv._2))))
    })

    //validate loglevel' value
    listOfKeys.foreach(kv => {
      val result: ResultSet = session.execute("select loglevel from redhatpoc.stack_services WHERE id=" + "'" + kv._1 + "';")
      val allresultsByKey: Seq[Row] = result.all().toIndexedSeq
      allresultsByKey.foreach(row => assertThat(row.getString(0), isOneOf("WARNING", "ERROR", "INFO")))
      allresultsByKey.foreach(row => assertThat(row.getString(0), not(isOneOf("emerg", "aler", "crit", "notice", "debug"))))
    })

    //validate timeframe < 1440
    listOfKeys.foreach(kv => {
      val result = session.execute("select timeframe from redhatpoc.stack_services WHERE id=" + "'" + kv._1 + "';")
      val allresultsByKey: Seq[Row] = result.all().toIndexedSeq
      allresultsByKey.foreach(row => assertThat(Integer.valueOf(row.getInt(0)), lessThanOrEqualTo(Integer.valueOf
      (1440))))
    })
  }


  /**
    * * Test: create and populate raw_logs
    */
  @Test(timeout = 60000)
  def testInyectRawLogData() = {

    val rawLogEntry: DataStream[Tuple7[String, String, String, String, String, Timestamp, String]] =
      logEntryToTupleRL(streamOfLogs, "boston")

    CassandraSink.addSink(rawLogEntry.javaStream)
      .setQuery("INSERT INTO redhatpoc.raw_logs (date, region, loglevel, service, node_type, log_ts, payload) VALUES " +
        "(?, ?,?, ?, ?, ?, ?);")
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

  /**
    * Test: create and populate couunters_nodes
    */
  @Test(timeout = 60000)
  def testInyectLogEntryNodeCounter() = {
    val listNodeCounter: Map[DataStream[Tuple5[String, String, String, String, String]], Int] = listOfKeys
      .map(e => (logEntryToTupleNC(streamOfLogs, e._1, "az1", "boston"), e._2))

    listNodeCounter.foreach(t => {
      CassandraSink.addSink(t._1.javaStream).setQuery("INSERT INTO redhatpoc" +
        ".counters_nodes (id, loglevel, az,region, node_type, ts) VALUES (?, ?, ?, ?, ?, now()) USING TTL " + t._2 + ";")
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
    })

    env.execute()

    val full = session.execute("select  * from redhatpoc.counters_nodes")
    val a: Array[AnyRef] = full.all().toArray()
    a.foreach(println)
  }

  /**
    * Test: create and populate services_counters
    */
  @Test(timeout = 60000)
  def testInyectLogEntryServicesCounter() = {
    val listServiceCounter: Map[DataStream[Tuple5[String, String, String, String, String]], Int] = listOfKeys
      .map(e => (logEntryToTupleSC(streamOfLogs, e._1, "az1", "boston"), e._2))

    listServiceCounter.foreach(t => {
      CassandraSink.addSink(t._1.javaStream).setQuery(
        "INSERT INTO redhatpoc.counters_services (id, loglevel, az, region, service, ts) VALUES (?, " +
          "?," +
          " " +
          "?," +
          " " +
          "?, " +
          "?, now()) USING TTL " + t._2 + ";")
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
    })

    env.execute()

    val full = session.execute("select  * from redhatpoc.counters_services")
    val a: Array[AnyRef] = full.all().toArray()
    a.foreach(println)
  }

}


