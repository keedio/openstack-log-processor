package com.keedio.flink

import java.sql.Timestamp

import com.datastax.driver.core.Cluster
import com.datastax.driver.core.Cluster.Builder
import org.apache.flink.api.java.tuple._
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}
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
    val stream: DataStream[String] = env
      .addSource(new FlinkKafkaConsumer08[String](
        parameterTool.getRequired("topic"), new SimpleStringSchema(), parameterTool.getProperties))

    //val streamOfLogs: DataStream[LogEntry] = stream.map(string => new LogEntry(string, Seq("date", "time", "pid", "loglevel")))

    //Load tables:
    val envBatch = ExecutionEnvironment.getExecutionEnvironment
    val tablesLoaded: DataSet[String] = envBatch.readTextFile("./src/main/resources/tables/tables.csv")
    val datasetTables: DataSet[DbTable] = tablesLoaded.map(s => new DbTable(s.split(";")(0), s.split(";").slice(1, s.size - 1): _*))



    val listOfKeys: Map[String, Int] = Map("1h" -> 3600, "6h" -> 21600, "12h" -> 43200, "24h" -> 86400, "1w" -> 604800, "1m" -> 2419200)

    val listNodeCounter: Map[DataStream[Tuple5[String, String, String, String, String]], Int] = listOfKeys
      .map(e => (stringToTupleNC(stream, e._1, "az1", "boston"), e._2))

    val listServiceCounter: Map[DataStream[Tuple5[String, String, String, String, String]], Int] = listOfKeys
      .map(e => (stringToTupleSC(stream, e._1, "az1", "boston"), e._2))

    val listStackService: Map[DataStream[Tuple5[String, String, String, String, Int]], Int] = listOfKeys
      .map(e => (stringToTupleSS(stream, e._1, e._2, "boston"), e._2))

    val rawLog: DataStream[Tuple7[String, String, String, String, String, Timestamp, String]] = stringToTupleRL(stream, "boston")


    //SINKING
    listNodeCounter.foreach { t => CassandraSink.addSink(t._1.javaStream)
      .setQuery("INSERT INTO redhatpoc.counters_nodes (id, loglevel, az, region, node_type, ts) VALUES (?, ?, ?, ?, ?, now()) USING TTL " + t._2 + ";")
      .setClusterBuilder(new ClusterBuilder() {
        override def buildCluster(builder: Builder): Cluster = {
          builder.addContactPoint(parameterTool.getRequired("cassandra.host")).build()
        }
      })
      .build()
    }

    listServiceCounter.foreach { t => CassandraSink.addSink(t._1.javaStream)
      .setQuery("INSERT INTO redhatpoc.counters_services (id, loglevel, az, region, service, ts) VALUES (?, ?, ?, ?, ?, now()) USING TTL " + t._2 + ";")
      .setClusterBuilder(new ClusterBuilder() {
        override def buildCluster(builder: Builder): Cluster = {
          builder.addContactPoint(parameterTool.getRequired("cassandra.host")).build()
        }
      })
      .build()
    }

    listStackService.foreach { t => CassandraSink.addSink(t._1.javaStream)
      .setQuery("INSERT INTO redhatpoc.stack_services (id, region, loglevel, service, ts, timeframe) VALUES (?, ?, ?, ?, now(), ?) USING TTL " + t._2 + ";")
      .setClusterBuilder(new ClusterBuilder() {
        override def buildCluster(builder: Builder): Cluster = {
          builder.addContactPoint(parameterTool.getRequired("cassandra.host")).build()
        }
      })
      .build()
    }

    CassandraSink.addSink(rawLog.javaStream)
      .setQuery("INSERT INTO redhatpoc.raw_logs (date, region, loglevel, service, node_type, log_ts, payload) VALUES (?, ?, ?, ?, ?, ?, ?);")
      .setClusterBuilder(new ClusterBuilder() {
        override def buildCluster(builder: Builder): Cluster = {
          builder.addContactPoint(parameterTool.getRequired("cassandra.host")).build()
        }
      })
      .build()


    env.execute()

  }


  /**
    * Extract value "log-level" form a common syslog line.
    * Value for log level info is expected to be the fourth word in a standarized syslog line.
    * If standard is not met, use argument 'exp' for marking off an expression.
    * Example:
    * - common syslog: "2017-02-10 06:18:07.264 3397 INFO eventlet.wsgi.server ...some other stuff"
    * - irregular syslog: "whatever myMachineName: 2017-02-10 06:18:07.264 3397 INFO eventlet.wsgi.server ...some other stuff"
    * exp = "myMachine:"
    *
    * @param s
    * @param exp
    * @return
    */
  def getFieldFromString(s: String, exp: String = "", position: Int): String = {
    var requiredValue: String = ""
    try {
      requiredValue = s.trim.split("\\s+")(position)
    } catch {
      case e: ArrayIndexOutOfBoundsException => LOG.error("Cannot parse string: does line contains loglevel info or timestamp? " + s)
    }
    requiredValue
  }

  /**
    * Function for transforming DataStream[String] to DataStream[Tuple5 o Tuple6]
    *
    * @param stream
    * @param timeKey
    * @param az
    * @param region
    * @return
    */
  def stringToTupleNC(stream: DataStream[String], timeKey: String, az: String, region: String): DataStream[Tuple5[String, String, String, String, String]] = {
    stream
      .map(string => {
        val logLevel: String = getFieldFromString(string, "", 3)
        val node = generateRandomNodeType
        new Tuple5(timeKey, logLevel, az, region, node)
      })
      .filter(t => t.f1 match {
        case "INFO" => true
        case "ERROR" => true
        case "WARNING" => true
        case _ => false
      })
  }

  def stringToTupleSC(stream: DataStream[String], timeKey: String, az: String, region: String): DataStream[Tuple5[String, String, String, String, String]] = {
    stream
      .map(string => {
        val logLevel: String = getFieldFromString(string, "", 3)
        //        val service: String = getFieldFromString(string, "", 4) match {
        //          case "" => "keystone"
        //          case _ => getFieldFromString(string, "", 4)
        //        }
        val service = generateRandomService
        new Tuple5(timeKey, logLevel, az, region, service)
      })
      .filter(t => t.f1 match {
        case "INFO" => true
        case "ERROR" => true
        case "WARNING" => true
        case _ => false
      })
  }

  def stringToTupleSS(stream: DataStream[String], timeKey: String, valKey: Int, region: String): DataStream[Tuple5[String, String, String, String, Int]] = {
    stream
      .map(string => {
        val logLevel: String = getFieldFromString(string, "", 3)
        val pieceTime: String = getFieldFromString(string, "", 1)
        val timeframe: Int = getMinutesFromTimePieceLogLine(pieceTime)
        val service = generateRandomService
        new Tuple5(timeKey, region, logLevel, service, timeframe)
      })
      .filter(t => t.f2 match {
        case "INFO" => true
        case "ERROR" => true
        case "WARNING" => true
        case _ => false
      })
      .filter(t => isValidTimeFrame(t.f4, valKey))
  }

  def stringToTupleRL(stream: DataStream[String], region: String): DataStream[Tuple7[String, String, String, String, String, Timestamp, String]] = {
    stream
      .map(string => {
        val logLevel: String = getFieldFromString(string, "", 3)
        val pieceDate: String = getFieldFromString(string, "", 0)
        //        val service: String = getFieldFromString(string, "", 4) match {
        //          case "" => "keystone"
        //          case _ => getFieldFromString(string, "", 4)
        //        }
        val service = generateRandomService
        val node_type = generateRandomNodeType
        val stringtimestamp: String = new String(getFieldFromString(string, "", 0) + " " + getFieldFromString(string, "", 1))
        var log_ts = new Timestamp(0L)
        try {
          log_ts = Timestamp.valueOf(stringtimestamp)
        } catch {
          case e: IllegalArgumentException => LOG.info("cannot create timestamp from string " + stringtimestamp)
        }
        val payload = string
        new Tuple7(pieceDate, region, logLevel, service, node_type, log_ts, payload)
      })
      .filter(t => t.f2 match {
        case "INFO" => true
        case "ERROR" => true
        case "WARNING" => true
        case _ => false
      })
  }


  /**
    * Get minutes from time token in syslog
    * 09:40 == 09*60 + 40
    *
    * @param pieceTime
    * @return
    */
  def getMinutesFromTimePieceLogLine(pieceTime: String): Int = {
    var pieceHour = 0
    var pieceMinute = 0
    try {
      pieceHour = pieceTime.split(":")(0).toInt * 60
      pieceMinute = pieceTime.split(":")(1).toInt
    } catch {
      case e: NumberFormatException => LOG.warn("String cannot be cast to Integer: " + pieceTime)
      case e: ArrayIndexOutOfBoundsException => LOG.warn("Malformed piece of time : " + pieceTime)
    }
    pieceHour + pieceMinute
  }

  /**
    * Only for poc
    */
  def generateRandomService: String = {
    val servicesMap = Map(
      0 -> "Nova",
      1 -> "Keystone",
      2 -> "Pacemaker",
      3 -> "Neutron",
      4 -> "Storage",
      5 -> "Cinder",
      6 -> "Glance",
      7 -> "Swift"
    )
    val rand = scala.util.Random
    val randKey = rand.nextInt(7)
    servicesMap(randKey)
  }

  /**
    * Only for poc
    */
  def generateRandomNodeType: String = {
    val servicesMap = Map(
      0 -> "compute",
      1 -> "storage"
    )
    val rand = scala.util.Random
    val randKey = rand.nextInt(1)
    servicesMap(randKey)
  }

  /**
    * The intention is to validate the log printed hour against the hour range they should belong to.
    * Specifically, if the hour range is  {1h, 6h, 12h, 24h, 1w, 1m}, each of this values but transformated into
    * seconds units would be the valkey and the “timeframe” then would be the log hour per 60 mins.
    * One log hour would be valid only if the timeframe between the log hour and current hour is whihin the framework that the “timeframe” field limits
    * For example:
    *   -    Current hour is 15:00 and the log is 08:00. Time difference would be 15-8=7 hours, this will render
    *       that all hour values would be acceptable but 1h and 6 h since this ones are above the range
    *   -    If current hour is 15:00 and log is 17:00, time difference would be then 24-17+15=22 hours
    *    in this case the acceptable values would be any but 1h, 6 h and 12h since these ones are abobe the range
    *
    * @param timeframe
    * @param valKey
    * @return
    */
  def isValidTimeFrame(timeframe: Int, valKey: Int, now: DateTime = DateTime.now()): Boolean = {
    val timeframeSeconds: Int = timeframe * 60
    val nowSeconds: Int = now.getHourOfDay * 3600 + now.getMinuteOfHour * 60 + now.getSecondOfMinute
    timeframeSeconds <= nowSeconds match {
      case true => (nowSeconds - timeframeSeconds) <= valKey
      case false => (24 * 60 * 60) - timeframeSeconds + nowSeconds <= valKey
    }
  }


}
