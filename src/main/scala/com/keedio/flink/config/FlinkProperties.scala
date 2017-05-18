package com.keedio.flink.config

import com.keedio.flink.utils.ProcessorHelper
import org.apache.flink.api.java.utils.ParameterTool

import scala.collection.JavaConverters._

/**
  * Created by luislazaro on 17/5/17.
  * lalazaro@keedio.com
  * Keedio
  */
class FlinkProperties(args: Array[String]) extends Serializable{
  require(!args.isEmpty)
  lazy val parameterToolCli: ParameterTool = ParameterTool.fromArgs(args)
  lazy val parameterToolFromFile: ParameterTool = parameterToolCli.getProperties.propertyNames().asScala.toSeq.contains("properties.file") match {
    case true => ParameterTool.fromPropertiesFile(parameterToolCli.get("properties.file"))
    case false => parameterToolCli
  }


  object FlinkProperties extends Serializable{
    lazy val CASSANDRAPORT = ProcessorHelper.getValueFromProperties(parameterToolCli, "cassandra.port",
      ProcessorHelper.getValueFromProperties(parameterToolFromFile, "cassandra.port", "9042"))

   lazy val RESTART_ATTEMPTS = ProcessorHelper.getValueFromProperties(parameterToolCli, "restart.attempts",
      ProcessorHelper.getValueFromProperties(parameterToolFromFile, "flink.strategy.restart.attempts", "3")).toInt

    lazy val RESTART_DELAY = ProcessorHelper.getValueFromProperties(parameterToolFromFile, "restart.delay",
      ProcessorHelper.getValueFromProperties(parameterToolFromFile, "flink.strategy.restart.delay", "10")).toInt

    lazy val MAXOUTOFORDENESS = ProcessorHelper.getValueFromProperties(parameterToolCli, "maxOutOfOrderness",
      ProcessorHelper.getValueFromProperties(parameterToolFromFile, "flink.assginerTimestamp.maxOutOfOrderness", "0")).toLong

    lazy val CHECKPOINT_INTERVAL = ProcessorHelper.getValueFromProperties(parameterToolCli, "checkpointInterval",
      ProcessorHelper.getValueFromProperties(parameterToolFromFile, "flink.checkpointing.interval", "100000")).toLong

    lazy val PARSEBODY: Boolean = ProcessorHelper.getValueFromProperties(parameterToolCli, "parseBody",
      ProcessorHelper.getValueFromProperties(parameterToolFromFile, "flink.parse.body", "true")).toBoolean

    lazy val CASSANDRAHOST = ProcessorHelper.getValueFromProperties(parameterToolCli, "cassandra.host",
      ProcessorHelper.getValueFromProperties(parameterToolFromFile, "cassandra.host", "disabled"))

    lazy val SOURCE_TOPIC = ProcessorHelper.getValueFromProperties(parameterToolCli, "source-topic",
      ProcessorHelper.getValueFromProperties(parameterToolFromFile, "source-topic", ""))

    lazy val TARGET_TOPIC = ProcessorHelper.getValueFromProperties(parameterToolCli, "topic-target",
      ProcessorHelper.getValueFromProperties(parameterToolFromFile, "target-topic", ""))

    lazy val BOOSTRAP_SERVERS = ProcessorHelper.getValueFromProperties(parameterToolCli, "bootstrap.servers",
      ProcessorHelper.getValueFromProperties(parameterToolFromFile, "bootstrap.servers", "127.0.0.1:9092"))

    lazy val ZOOKEEPER_CONNECT = ProcessorHelper.getValueFromProperties(parameterToolCli, "zookeeper.connect",
      ProcessorHelper.getValueFromProperties(parameterToolFromFile, "zookeeper.connect", "127.0.0.1:2181"))

    lazy val GROUP_ID: String = ProcessorHelper.getValueFromProperties(parameterToolCli, "group.id",
      ProcessorHelper.getValueFromProperties(parameterToolFromFile, "group.id", "myGroup"))

    lazy val AUTO_OFFSET_RESET = ProcessorHelper.getValueFromProperties(parameterToolCli, "auto.offset.reset",
      ProcessorHelper.getValueFromProperties(parameterToolFromFile, "auto.offset.reset", "latest"))

    lazy val BROKER = ProcessorHelper.getValueFromProperties(parameterToolCli, "broker",
      ProcessorHelper.getValueFromProperties(parameterToolFromFile, "broker", BOOSTRAP_SERVERS))

    lazy val parameterTool: ParameterTool = parameterToolFromFile.mergeWith(parameterToolCli)
  }

}