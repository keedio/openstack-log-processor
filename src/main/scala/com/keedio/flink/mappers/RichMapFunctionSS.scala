package com.keedio.flink.mappers

import com.keedio.flink.entities.LogEntry
import com.keedio.flink.utils.{ProcessorHelper, SyslogCode}
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.java.tuple.Tuple7
import org.apache.flink.configuration.Configuration
import org.apache.flink.metrics.Counter

/**
  * Created by luislazaro on 28/3/17.
  * lalazaro@keedio.com
  * Keedio
  */
class RichMapFunctionSS(val timeKey: String, val valKey: Int, val region: String)
  extends RichMapFunction[LogEntry, Tuple7[String, String, String, String, Int, String, Int]] {

  var counter: Counter = _

  @throws(classOf[Exception])
  override def map(logEntry: LogEntry): Tuple7[String, String, String, String, Int, String, Int] = {
    counter.inc()
    val logLevel: String = SyslogCode.severity(logEntry.severity)
    val timeframe: Int = ProcessorHelper.getTimeFrameMinutes(logEntry.timestamp)
    val service = logEntry.service
    val ttl: Int = ProcessorHelper.computeTTL(logEntry.timestamp, valKey)
    new Tuple7(timeKey, region, logLevel, service, timeframe, logEntry.timestamp, ttl)
  }

  override def open(configuration: Configuration) = {
    this.counter = getRuntimeContext
      .getMetricGroup
      .addGroup("MyMetrics")
      .counter("Total_LogEntry_To_Stack_Services")
  }

}

