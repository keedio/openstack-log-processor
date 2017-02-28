package com.keedio.flink

import java.sql.Timestamp

import com.datastax.driver.core.Cluster
import com.datastax.driver.core.Cluster.Builder
import com.datastax.driver.core.exceptions.DriverException
import com.keedio.flink.utils.{ProcessorHelper, ProcessorHelperPoc}
import org.apache.flink.api.java.tuple._
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala.{DataStream, _}
import org.apache.flink.streaming.connectors.cassandra.{CassandraSink, ClusterBuilder}
import org.apache.flink.streaming.connectors.kafka._
import org.apache.flink.streaming.util.serialization._
import org.apache.log4j.Logger

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

    //will populate tables basis on column id : 1h, 6h, ...
    val listOfKeys: Map[String, Int] = Map("1h" -> 3600, "6h" -> 21600, "12h" -> 43200, "24h" -> 86400, "1w" ->
      604800, "1m" -> 2419200)

    //Create a stream of data for each id and map that stream to a specific flink.tuple.
    val listNodeCounter: Map[DataStream[Tuple5[String, String, String, String, String]], Int] = listOfKeys
      .map(e => (stringToTupleNC(stream, e._1, "az1", "boston"), e._2))

    val listServiceCounter: Map[DataStream[Tuple5[String, String, String, String, String]], Int] = listOfKeys
      .map(e => (stringToTupleSC(stream, e._1, "az1", "boston"), e._2))

    val listStackService: Iterable[DataStream[Tuple7[String, String, String, String, Int,String, Int]]] = listOfKeys
      .map(e => stringToTupleSS(stream, e._1, e._2, "boston"))

    val rawLog: DataStream[Tuple7[String, String, String, String, String, Timestamp, String]] =stringToTupleRL(stream, "boston")

    //SINKING
    listNodeCounter.foreach { t => {
      {
        {
          CassandraSink.addSink(t._1.javaStream)
            .setQuery("INSERT INTO redhatpoc.counters_nodes (id, loglevel, az, region, node_type, ts) VALUES (?, ?, " +
              "?, " +
              "?," +
              " " +
              "?, now()) USING TTL " + t._2 + ";")
            .setClusterBuilder(new ClusterBuilder() {
              override def buildCluster(builder: Builder): Cluster = {
                builder.addContactPoint(parameterTool.getRequired("cassandra.host")).build()
              }
            })
            .build()
        }
      }
    }
    }

    listServiceCounter.foreach { t => {
      {
        {
          CassandraSink.addSink(t._1.javaStream)
            .setQuery("INSERT INTO redhatpoc.counters_services (id, loglevel, az, region, service, ts) VALUES (?, ?, " +
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
        }
      }
    }
    }

    listStackService.foreach { t => {
      {
        {
          CassandraSink.addSink(t.javaStream)
            .setQuery("INSERT INTO redhatpoc.stack_services (id, region, loglevel, service, ts, timeframe, tfHours) VALUES (?,?,?,?, now(),?,?) USING TTL " + "?" + ";")
            .setClusterBuilder(new ClusterBuilder() {
              override def buildCluster(builder: Builder): Cluster = {
                builder.addContactPoint(parameterTool.getRequired("cassandra.host")).build()
              }
            })
            .build()
        }
      }
    }
    }

    CassandraSink.addSink(rawLog.javaStream)
      .setQuery("INSERT INTO redhatpoc.raw_logs (date, region, loglevel, service, node_type, log_ts, payload) VALUES " +
        "(?, ?, ?, ?, ?, ?, ?);")
      .setClusterBuilder(new ClusterBuilder() {
        override def buildCluster(builder: Builder): Cluster = {
          builder.addContactPoint(parameterTool.getRequired("cassandra.host")).build()
        }
      })
      .build()

    try {
    env.execute()
  } catch {
      case e : DriverException => LOG.error("", e)
    }

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
  def stringToTupleNC(stream: DataStream[String], timeKey: String, az: String, region: String):
  DataStream[Tuple5[String, String, String, String, String]] = {
    stream
      .map(string => {
        val logLevel: String = ProcessorHelper.getFieldFromString(string, "", 3)
        val node = ProcessorHelperPoc.generateRandomNodeType
        new Tuple5(timeKey, logLevel, az, region, node)
      })
      .filter(t => {
        t.f1 match {
          case "INFO" => true
          case "ERROR" => true
          case "WARNING" => true
          case _ => false
        }
      })
  }

  def stringToTupleSC(stream: DataStream[String], timeKey: String, az: String, region: String):
  DataStream[Tuple5[String, String, String, String, String]] = {
    stream
      .map(string => {
        val logLevel: String = ProcessorHelper.getFieldFromString(string, "", 3)
        //        val service: String = getFieldFromString(string, "", 4) match {
        //          case "" => "keystone"
        //          case _ => getFieldFromString(string, "", 4)
        //        }
        val service = ProcessorHelperPoc.generateRandomService
        new Tuple5(timeKey, logLevel, az, region, service)
      })
      .filter(t => {
        t.f1 match {
          case "INFO" => true
          case "ERROR" => true
          case "WARNING" => true
          case _ => false
        }
      })
  }

  def stringToTupleSS(stream: DataStream[String], timeKey: String, valKey: Int, region: String):
  DataStream[Tuple7[String, String, String, String, Int,String, Int]] = {
    stream
      .map(string => {
        val logLevel: String = ProcessorHelper.getFieldFromString(string, "", 3).trim
        val pieceTime: String = ProcessorHelper.getFieldFromString(string, "", 1).trim
        val timeframe: Int = ProcessorHelper.getMinutesFromTimePieceLogLine(pieceTime)
        val service = ProcessorHelperPoc.generateRandomService
        val ttl: Int = ProcessorHelper.computeTTL(timeframe, valKey)
        new Tuple7(timeKey, region, logLevel, service, timeframe,pieceTime,ttl)
      })
      .filter(t =>
        t.f2 match {
          case "INFO" => true
          case "ERROR" => true
          case "WARNING" => true
          case _ => false
        })
      .filter(t => ProcessorHelper.isValidTimeFrame(t.f4, valKey))
  }

  def stringToTupleRL(stream: DataStream[String], region: String): DataStream[Tuple7[String, String, String, String,
    String, Timestamp, String]] = {
    stream
      .map(string => {
        val logLevel: String = ProcessorHelper.getFieldFromString(string, "", 3)
        val pieceDate: String = ProcessorHelper.getFieldFromString(string, "", 0)
        //        val service: String = getFieldFromString(string, "", 4) match {
        //          case "" => "keystone"
        //          case _ => getFieldFromString(string, "", 4)
        //        }
        val service = ProcessorHelperPoc.generateRandomService
        val node_type = ProcessorHelperPoc.generateRandomNodeType
        val stringtimestamp: String = new String(ProcessorHelper.getFieldFromString(string, "", 0) + " " +
          ProcessorHelper.getFieldFromString(string, "", 1))
        var log_ts = new Timestamp(0L)
        try {
          log_ts = Timestamp.valueOf(stringtimestamp)
        } catch {
          case e: IllegalArgumentException => LOG.info("cannot create timestamp from string " + stringtimestamp)
        }
        val payload = string
        new Tuple7(pieceDate, region, logLevel, service, node_type, log_ts, payload)
      })
      .filter(t => {
        t.f2 match {
          case "INFO" => true
          case "ERROR" => true
          case "WARNING" => true
          case _ => false
        }
      })
  }
}
