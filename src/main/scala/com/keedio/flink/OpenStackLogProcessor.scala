package com.keedio.flink

import com.datastax.driver.core.Cluster
import com.datastax.driver.core.Cluster.Builder
import com.keedio.flink.dbmodels._
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
    stream.rebalance.print

    val parsedStreamNC: DataStream[NodesCounter] = stream.map(string => {
      val keyAndVal = string.split(":")
      val columns: Array[String] = keyAndVal(1).split("\\s+")
      val logLevel = columns(3)
      new NodesCounter("1h", logLevel,"az1","boston","compute", "now()" )
    })

    parsedStreamNC.filter(nodesCounter => nodesCounter.getLoglevel match {
      case "INFO" => true
      case "ERROR" => true
      case "WARNING" => true
      case _ => false
    })

    //sink cassandra
    val INSERT = "INSERT INTO redhatpoc.cassandraconnectorexample (id, text) VALUES (?, ?)"
    val list: List[Tuple2[String, String]] = List(new Tuple2("1", "a"), new Tuple2("2", "b"), new Tuple2("3", "c"))
    val source: DataStream[Tuple2[String, String]] = env.fromCollection(list)
    CassandraSink.addSink(source.javaStream)
      .setQuery(INSERT)
      .setClusterBuilder(new ClusterBuilder() {
        override def buildCluster(builder: Builder): Cluster = {
          builder.addContactPoint("127.0.0.1").build()
        }
      })
      .build()

    env.execute()

  }
}
