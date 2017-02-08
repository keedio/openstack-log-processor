package com.keedio.flink

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka._
import org.apache.flink.streaming.util.serialization._

/**
  * Created by luislazaro on 8/2/17.
  * lalazaro@keedio.com
  * Keedio
  */
object KafkaLogProcessor {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val parameterTool = ParameterTool.fromArgs(args);

    val stream: DataStream[String] = env
      .addSource(new FlinkKafkaConsumer08[String](
        parameterTool.getRequired("topic"), new SimpleStringSchema(), parameterTool.getProperties))
    stream.rebalance.print
    env.execute()
  }
}
