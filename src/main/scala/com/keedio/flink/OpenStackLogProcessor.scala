package com.keedio.flink

import com.datastax.driver.core.Cluster
import com.datastax.driver.core.Cluster.Builder
import com.datastax.driver.core.utils.UUIDs
import org.apache.flink.api.java.tuple._
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.cassandra.{CassandraSink, ClusterBuilder}
import org.apache.flink.streaming.connectors.kafka._
import org.apache.flink.streaming.util.serialization._

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

    //requerir rebalance para que hay sink unamed (no confundir con cassandra sink)
    //stream.rebalance.print

    val parsedStreamNC: DataStream[Tuple6[String, String, String, String, String, String]] = stream.map(string => {
      val keyAndVal = string.split("root:")

      var columns: Array[String] = Array()
      var logLevel: String = ""
      try {
        columns = keyAndVal(1).split("\\s+")
        logLevel = columns(4)
      } catch {
        case e: ArrayIndexOutOfBoundsException => println("Cannot parse string: no loglevel info? ------------------------> " + string)
      }
      new Tuple6("1h", logLevel, "az1", "boston", "compute", UUIDs.timeBased().toString)
    })

    parsedStreamNC.map(t => println(t.toString))

    val source: DataStream[Tuple6[String, String, String, String, String, String]] = parsedStreamNC.filter(t => t.f1 match {
      case "INFO" => true
      case "ERROR" => true
      case "WARNING" => true
      case _ => false
    })

    source.rebalance.print

    //sink cassandra
    CassandraSink.addSink(source.javaStream)
      .setQuery("INSERT INTO redhatpoc.counters_nodes_test (id, loglevel, az, region, node_type, ts) VALUES (?, ?, ?, ?, ?, ?) USING TTL 20;")
      .setClusterBuilder(new ClusterBuilder() {
        override def buildCluster(builder: Builder): Cluster = {
          builder.addContactPoint(parameterTool.getRequired("cassandra.host")).build()
        }
      })
      .build()

    source.javaStream.rebalance().print
    env.execute()

  }
}
