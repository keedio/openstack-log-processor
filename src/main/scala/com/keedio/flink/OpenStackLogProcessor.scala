package com.keedio.flink

import java.sql.Timestamp
import java.util.concurrent.TimeUnit

import com.datastax.driver.core.Cluster
import com.datastax.driver.core.Cluster.Builder
import com.datastax.driver.core.exceptions.DriverException
import com.keedio.flink.cep.alerts.ErrorAlert
import com.keedio.flink.cep.patterns.ErrorAlertCreateVMPattern
import com.keedio.flink.cep.{IAlert, IAlertPattern}
import com.keedio.flink.entities.LogEntry
import com.keedio.flink.mappers._
import com.keedio.flink.utils._
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.tuple._
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.cep.PatternSelectFunction
import org.apache.flink.cep.scala.{CEP, PatternStream}
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.{createTypeInformation, _}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.connectors.cassandra.{CassandraSink, ClusterBuilder}
import org.apache.flink.streaming.connectors.kafka._
import org.apache.flink.streaming.util.serialization._
import org.apache.log4j.Logger

import scala.collection.JavaConverters._
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
    //From the command line arguments
    val parameterTool: ParameterTool = ParameterTool.fromArgs(args)
    val CASSANDRAPORT: Int = ProcessorHelper.getValueFromArgs(parameterTool, "cassandra.port", "9042").toInt
    val RESTART_ATTEMPTS = ProcessorHelper.getValueFromArgs(parameterTool, "restart.attempts", "3").toInt
    val RESTART_DELAY = ProcessorHelper.getValueFromArgs(parameterTool, "restart.delay", "10").toInt
    val MAXOUTOFORDENESS = ProcessorHelper.getValueFromArgs(parameterTool, "maxOutOfOrderness", "0").toLong

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.enableCheckpointing(100000)
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(
      RESTART_ATTEMPTS,
      org.apache.flink.api.common.time.Time.of(RESTART_DELAY, TimeUnit.MINUTES)
    ))
    // Use the Measurement Timestamp of the Event (set a notion of time)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    //source of data is Kafka. We subscribe as consumer via connector FlinkKafkaConsumer08
    val kafkaConsumer: FlinkKafkaConsumer08[String] = new FlinkKafkaConsumer08[String](
      parameterTool.getRequired("topic"), new SimpleStringSchema(), parameterTool.getProperties)

    val stream: DataStream[String] = env.addSource(kafkaConsumer)

    //parse jsones as logentries
    val streamOfLogs: DataStream[LogEntry] = stream
      .map(s => LogEntry(s, parameterTool.getBoolean("parseBody", true)))
      .filter(logEntry => logEntry.isValid())
      .filter(logEntry => SyslogCode.acceptedLogLevels.contains(SyslogCode(logEntry.severity)))
      .rebalance

    //assign and emit watermarks: events may arrive unordered
    val streamOfLogsTimestamped: DataStream[LogEntry] = streamOfLogs.assignTimestampsAndWatermarks(
      new BoundedOutOfOrdernessTimestampExtractor[LogEntry](Time.seconds(MAXOUTOFORDENESS)) {
        override def extractTimestamp(t: LogEntry): Long = ProcessorHelper.toTimestamp(t.timestamp).getTime
      })
      .setParallelism(1)

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
      logEntryToTupleRL(streamOfLogs, "boston")

    //SINKING
    listNodeCounter.foreach(t => {
      CassandraSink.addSink(t._1.javaStream).setQuery("INSERT INTO redhatpoc" +
        ".counters_nodes (id, loglevel, az, " +
        "region, node_type, ts) VALUES (?, ?, ?, ?, ?, now()) USING TTL " + t._2 + ";")
        .setClusterBuilder(new ClusterBuilder() {
          override def buildCluster(builder: Builder): Cluster = {
            builder
              .addContactPoint(parameterTool.getRequired("cassandra.host"))
              .withPort(CASSANDRAPORT)
              .build()
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
            builder
              .addContactPoint(parameterTool.getRequired("cassandra.host"))
              .withPort(CASSANDRAPORT)
              .build()
          }
        })
        .build()
    })

    /**
      * COLS:   |  id    |service|  loglevel|  region| ts   | tfhours   | timeframe| TTL(hiden)
      * VALUES: |  ?     | ?     |    ?     |    ?   | now()|    ?      |    ?     |    ?
      * TUPLE:  | timekey|service|  loglevel|  region|      | timestamp | timeframe|    ttl
      */
    listStackService.foreach(t => {
      CassandraSink.addSink(t.javaStream).setQuery("INSERT INTO redhatpoc.stack_services (id, region, loglevel, " +
        "service, ts, " +
        "timeframe, " + "tfHours)" + " " + "VALUES " + "(?,?,?,?, now(),?,?) USING TTL " + "?" + ";")
        .setClusterBuilder(new ClusterBuilder() {
          override def buildCluster(builder: Builder): Cluster = {
            builder
              .addContactPoint(parameterTool.getRequired("cassandra.host"))
              .withPort(CASSANDRAPORT)
              .build()
          }
        })
        .build()
    })

    CassandraSink.addSink(rawLog.javaStream).setQuery(
      "INSERT INTO redhatpoc.raw_logs (date, region, loglevel, service, node_type, log_ts, payload) " +
        "VALUES " + "(?, ?, ?, ?, ?, ?, ?);")
      .setClusterBuilder(new ClusterBuilder() {
        override def buildCluster(builder: Builder): Cluster = {
          builder
            .addContactPoint(parameterTool.getRequired("cassandra.host"))
            .withPort(CASSANDRAPORT)
            .build()
        }
      })
      .build()


    //CEP
    val streamOfErrorAlerts: DataStream[ErrorAlert] = toAlertStream(streamOfLogsTimestamped, new ErrorAlertCreateVMPattern)
    val streamErrorString: DataStream[String] = streamOfErrorAlerts.map(errorAlert => errorAlert.toString).setParallelism(1)
    val myProducer = new FlinkKafkaProducer08[String](
      parameterTool.getRequired("broker"), parameterTool.getRequired("target-topic"), new SimpleStringSchema())
    // the following is necessary for at-least-once delivery guarantee
    myProducer.setLogFailuresOnly(false) // "false" by default
    myProducer.setFlushOnCheckpoint(false) // "false" by defaultF
    streamErrorString.addSink(myProducer)

    //properties of job client
    val propertiesNames = parameterTool.getProperties.propertyNames().asScala.toSeq
    val listPropertiesFromCli: Seq[String] = propertiesNames.map(key => s" ${key}  : " + parameterTool.getProperties.getProperty(key.toString))

    try {
      env.execute(s"OpensStack Log Processor - " + listPropertiesFromCli.mkString(";"))
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
  def logEntryToTupleNC(
                         streamOfLogs: DataStream[LogEntry], timeKey: String, az: String,
                         region: String): DataStream[Tuple5[String, String, String, String, String]] = {
    streamOfLogs.map(new RichMapFunctionNC(timeKey, az, region))
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
    streamOfLogs.map(new RichMapFunctionSC(timeKey, az, region))
  }

  /**
    * function to map from Datastream of Logentry to Tuple raw logs
    *
    * @param streamOfLogs
    * @param region
    * @return
    */
  def logEntryToTupleRL(streamOfLogs: DataStream[LogEntry], region: String): DataStream[Tuple7[String, String,
    String, String, String, Timestamp, String]] = {
    streamOfLogs.map(new RichMapFunctionRL(region))
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
      .filter(logEntry => ProcessorHelper.isValidPeriodTime(logEntry.timestamp, valKey))
      .map(new RichMapFunctionSS(timeKey, valKey, region))
      .filter(t => t.f6 > 0)
  }

  /**
    * Generate DataSteam of Alerts
    *
    * @param streamOfLogsTimestamped
    * @param alertPattern
    * @param typeInfo
    * @tparam T
    * @return
    */
  def toAlertStream[T <: IAlert](streamOfLogsTimestamped: DataStream[LogEntry], alertPattern: IAlertPattern[LogEntry, T])
                                (implicit typeInfo: TypeInformation[T]): DataStream[T] = {
    val tempPatternStream: PatternStream[LogEntry] = CEP.pattern(streamOfLogsTimestamped,
      alertPattern.getEventPattern())
    val alerts: DataStream[T] = tempPatternStream.select(new PatternSelectFunction[LogEntry, T] {
      override def select(map: java.util.Map[String, LogEntry]): T = alertPattern.create(map)
    })
    alerts
  }

  /**
    * Generate DataStream of late elements
    */
  def toLateElementsStream[T <: IAlert](tag: String, streamOfLogsTimestamped: DataStream[LogEntry], alertPattern: IAlertPattern[LogEntry, T])
                                       (implicit typeInfo: TypeInformation[T]): DataStream[LogEntry] = {
    val lateOutputTag: OutputTag[LogEntry] = new OutputTag[LogEntry](tag)
    val tempPatternStream: PatternStream[LogEntry] = CEP.pattern(streamOfLogsTimestamped,
      alertPattern.getEventPattern())
      .sideOutputLateData(lateOutputTag)

    val alerts: DataStream[T] = tempPatternStream.select(new PatternSelectFunction[LogEntry, T] {
      override def select(map: java.util.Map[String, LogEntry]): T = alertPattern.create(map)
    })

    val lateStream: DataStream[LogEntry] = tempPatternStream.getSideOutput(lateOutputTag)
    lateStream
  }

}



