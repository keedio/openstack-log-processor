package com.keedio.flink

import java.sql.Timestamp

import com.datastax.driver.core.Cluster
import com.datastax.driver.core.Cluster.Builder
import com.datastax.driver.core.exceptions.DriverException
import com.keedio.flink.entities.LogEntry
import com.keedio.flink.utils._
import org.apache.flink.api.java.tuple._
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala.{DataStream, _}
import org.apache.flink.streaming.connectors.cassandra.{CassandraSink, ClusterBuilder}
import org.apache.flink.streaming.connectors.kafka._
import org.apache.flink.streaming.util.serialization._
import org.apache.log4j.Logger
import org.joda.time.DateTime

import scala.collection.Map

/**
  * Created by luislazaro on 8/2/17.
  * lalazaro@keedio.com
  * Keedio
  */
class OpenStackLogProcessor

object OpenStackLogProcessor {
  val LOG: Logger = Logger.getLogger(classOf[OpenStackLogProcessor])

  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val parameterTool = ParameterTool.fromArgs(args)

    //source of data is Kafka. We subscribe as consumer via connector FlinkKafkaConsumer08
    val stream: DataStream[String] = env
      .addSource(new FlinkKafkaConsumer08[String](
        parameterTool.getRequired("topic"), new SimpleStringSchema(), parameterTool.getProperties))

    //parse string to logentry entitie
    val streamOfLogs: DataStream[LogEntry] = stream.map(string => LogEntry(string))

    //will populate tables basis on column id : 1h, 6h, ...
    val listOfKeys: Map[String, Int] = Map("1h" -> 3600, "6h" -> 21600, "12h" -> 43200, "24h" -> 86400, "1w" ->
      604800, "1m" -> 2419200)

    //Create a stream of data for each id and map that stream to a specific flink.tuple.
    val listNodeCounter: Map[DataStream[Tuple5[String, String, String, String, String]], Int] = listOfKeys
      .map(e => (logEntryToTupleNC(streamOfLogs, e._1, "az1", "boston"), e._2))

    val listServiceCounter: Map[DataStream[Tuple5[String, String, String, String, String]], Int] = listOfKeys
      .map(e => (logEntryToTupleSC(streamOfLogs, e._1, "az1", "boston"), e._2))

    val listStackService: Iterable[DataStream[Tuple7[String, String, String, String, Int, String, Int]]] = listOfKeys
      .map(e => logEntryToTupleSS(streamOfLogs, e._1, e._2, "boston"))

    val rawLog: DataStream[Tuple7[String, String, String, String, String, Timestamp, String]] =
      logEntryToTupleRL(streamOfLogs,"boston")

    //SINKING
    listNodeCounter.foreach(t => {
      CassandraSink.addSink(t._1.javaStream).setQuery("INSERT INTO redhatpoc" +
        ".counters_nodes (id, loglevel, az, " +
        "region, node_type, ts) VALUES (?, ?, ?, ?, ?, now()) USING TTL " + t._2 + ";")
        .setClusterBuilder(new ClusterBuilder() {
          override def buildCluster(builder: Builder): Cluster = {
            builder.addContactPoint(parameterTool.getRequired("cassandra.host")).build()
          }
        })
        .build()
    })

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
            builder.addContactPoint(parameterTool.getRequired("cassandra.host")).build()
          }
        })
        .build()
    })

    listStackService.foreach(t => {
      CassandraSink.addSink(t.javaStream).setQuery("INSERT INTO redhatpoc.stack_services (id, region, loglevel, " +
        "service, ts, " +
        "timeframe, " + "tfHours)" + " " + "VALUES " + "(?,?,?,?, now(),?,?) USING TTL " + "?" + ";")
        .setClusterBuilder(new ClusterBuilder() {
          override def buildCluster(builder: Builder): Cluster = {
            builder.addContactPoint(parameterTool
              .getRequired("cassandra.host")).build()
          }
        })
        .build()
    })

    CassandraSink.addSink(rawLog.javaStream).setQuery(
      "INSERT INTO redhatpoc.raw_logs (date, region, loglevel, service, node_type, log_ts, payload) " +
        "VALUES " + "(?, ?, ?, ?, ?, ?, ?);")
      .setClusterBuilder(new ClusterBuilder() {
        override def buildCluster(builder: Builder): Cluster = {
          builder.addContactPoint(parameterTool.getRequired("cassandra.host")).build()
        }
      })
      .build()

    //execute
    try {
      env.execute()
    } catch {
      case e: DriverException => LOG.error("", e)
    }
  }

/**
* Function to map from DataStream of LogEntry to Tuple of node counters
  *
  * @param streamOfLogs
* @param timeKey
* @param az
* @param region
* @return
  */
  def logEntryToTupleNC(streamOfLogs: DataStream[LogEntry], timeKey: String, az: String, region: String):
  DataStream[Tuple5[String, String, String, String, String]] = {
    streamOfLogs
      .filter(logEntry => logEntry.isValid())
      .filter(logEntry => SyslogCode.acceptedLogLevels.contains(SyslogCode(logEntry.severity)))
      .map(logEntry => {
        val logLevel: String = SyslogCode.severity(logEntry.severity)
        val node = logEntry.hostname
        new Tuple5(timeKey, logLevel, az, region, node)
      })
  }

/**
* Function to map from DataStream of LogEntry to Tupe of service counters
  *
  * @param streamOfLogs
* @param timeKey
* @param az
* @param region
* @return
  */
  def logEntryToTupleSC(streamOfLogs: DataStream[LogEntry], timeKey: String, az: String, region: String):
  DataStream[Tuple5[String, String, String, String, String]] = {
    streamOfLogs
      .filter(logEntry => logEntry.isValid())
      .filter(logEntry => SyslogCode.acceptedLogLevels.contains(SyslogCode(logEntry.severity)))
      .map(logEntry => {
        val logLevel: String = SyslogCode.severity(logEntry.severity)
        val service = logEntry.service
        new Tuple5(timeKey, logLevel, az, region, service)
    })
  }

/**
* function to map from Datastream of Logentry to Tuple raw logs
  *
  * @param streamOfLogs
* @param region
* @return
  */
  def logEntryToTupleRL(streamOfLogs: DataStream[LogEntry], region: String): DataStream[Tuple7[String, String,
    String, String,String, Timestamp, String]] = {
    streamOfLogs
      .filter(logEntry => logEntry.isValid())
      .filter(logEntry => SyslogCode.acceptedLogLevels.contains(SyslogCode(logEntry.severity)))
      .map(logEntry => {
        val logLevel: String = SyslogCode.severity(logEntry.severity)
        val service = logEntry.service
        val node_type = logEntry.hostname
        //val pieceDate: String = logEntry.timestamp.split("\\s+")(0)
        val timestamp: DateTime = ProcessorHelper.getParsedTimestamp(logEntry.timestamp)
        val pieceDate: String = new String(timestamp.getYear.toString + "-" + timestamp.getMonthOfYear.toString + "-" +
          timestamp.getDayOfMonth.toString)
        var log_ts = new Timestamp(0L)
        try {
          log_ts = Timestamp.valueOf(logEntry.timestamp)
        } catch {
          case e: IllegalArgumentException => LOG.info("cannot create timestamp from string " + logEntry.timestamp)
        }
        new Tuple7(pieceDate, region, logLevel, service, node_type, log_ts, logEntry.toString)
      })
  }

  /**
    * Create a Tuple for stack_services from LogEntry
    *
    * @param streamOfLogs
    * @param timeKey
    * @param valKey
    * @param region
    * @return
    */
  def logEntryToTupleSS(streamOfLogs: DataStream[LogEntry], timeKey: String, valKey: Int, region: String):
  DataStream[Tuple7[String, String, String, String, Int, String, Int]] = {
    streamOfLogs
      .filter(logEntry => logEntry.isValid())
      .filter(logEntry => SyslogCode.acceptedLogLevels.contains(SyslogCode(logEntry.severity)))
      .filter(logEntry => ProcessorHelper.isValidPeriodTime(logEntry.timestamp, valKey))
      .map(logEntry => {
        val logLevel: String = SyslogCode.severity(logEntry.severity)
        //val pieceTime: String = logEntry.timestamp.split("\\s+")(1)
        val timeframe: Int = ProcessorHelper.getTimeFrameMinutes(logEntry.timestamp)
        //val timeframe: Int = ProcessorHelper.getMinutesFromTimePieceLogLine(pieceTime)
        val service = logEntry.service
        val ttl: Int = ProcessorHelper.computeTTLperiod(logEntry.timestamp , valKey)
        new Tuple7(timeKey, region, logLevel, service, timeframe, logEntry.timestamp, ttl)
      })
      .filter(t => t.f6 > 0)
  }

  }



