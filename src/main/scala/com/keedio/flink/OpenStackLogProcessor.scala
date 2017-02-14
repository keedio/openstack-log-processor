package com.keedio.flink

import com.datastax.driver.core.Cluster
import com.datastax.driver.core.Cluster.Builder
import org.apache.flink.api.java.tuple._
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala.{DataStream, _}
import org.apache.flink.streaming.connectors.cassandra.{CassandraSink, ClusterBuilder}
import org.apache.flink.streaming.connectors.kafka._
import org.apache.flink.streaming.util.serialization._

import scala.collection.Map

/**
  * Created by luislazaro on 8/2/17.
  * lalazaro@keedio.com
  * Keedio
  */
object OpenStackLogProcessor {

  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val parameterTool = ParameterTool.fromArgs(args)
    val stream: DataStream[String] = env
      .addSource(new FlinkKafkaConsumer08[String](
        parameterTool.getRequired("topic"), new SimpleStringSchema(), parameterTool.getProperties))

    val listOfKeys: Map[String, Int] = Map("1h" -> 3600, "6h" -> 21600, "12h" -> 43200, "24h" -> 86400, "1w" -> 604800, "1m" -> 2419200)

    val listNodeCounter: Map[DataStream[Tuple5[String, String, String, String, String]], Int] = listOfKeys
      .map(e => (stringToTuple5(stream, e._1, "az1", "boston", "compute"), e._2))

    val listServiceCounter: Map[DataStream[Tuple5[String, String, String, String, String]], Int] = listOfKeys
      .map(e => (stringToTuple5(stream, e._1, "az1", "boston", "keystone"), e._2))

    val listStackService: Map[DataStream[Tuple4[String, String, String, String]], Int] = listOfKeys
      .map(e => (stringToTuple4(stream, e._1, "boston", "keystone"), e._2))

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
      .setQuery("INSERT INTO redhatpoc.stack_services (id, region, loglevel, service, ts) VALUES (?, ?, ?, ?, now()) USING TTL " + t._2 + ";")
      .setClusterBuilder(new ClusterBuilder() {
        override def buildCluster(builder: Builder): Cluster = {
          builder.addContactPoint(parameterTool.getRequired("cassandra.host")).build()
        }
      })
      .build()
    }

    //source.javaStream.rebalance().print
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
  def getLogLevelFromString(s: String, exp: String = ""): String = {
    var logLevel: String = ""
    try {
      logLevel = exp match {
        case "" => s.split("\\s+")(3)
        case _ => s.split(exp)(1).split("\\s+")(4)
      }
    } catch {
      case e: ArrayIndexOutOfBoundsException => println("Cannot parse string: no loglevel info? ------------------------> " + s)
    }
    logLevel
  }

  /**
    * Function for transforming DataStream[String] to DataStream[Tuple5 o Tuple6]
    *
    * @param stream
    * @param timeKey
    * @param az
    * @param region
    * @param node_service
    * @return
    */

  def stringToTuple5(stream: DataStream[String], timeKey: String, az: String, region: String, node_service: String):
  DataStream[Tuple5[String, String, String, String, String]] = {
    stream
      .map(string => {
        val logLevel: String = getLogLevelFromString(string, "root:")
        new Tuple5(timeKey, logLevel, az, region, node_service)
      })
      .filter(t => t.f1 match {
        case "INFO" => true
        case "ERROR" => true
        case "WARNING" => true
        case _ => false
      })
  }

  def stringToTuple4(stream: DataStream[String], timeKey: String, region: String, node_service: String):
  DataStream[Tuple4[String, String, String, String]] = {
    stream
      .map(string => {
        val logLevel: String = getLogLevelFromString(string, "root:")
        new Tuple4(timeKey, logLevel, region, node_service)
      })
      .filter(t => t.f1 match {
        case "INFO" => true
        case "ERROR" => true
        case "WARNING" => true
        case _ => false
      })
  }

}
