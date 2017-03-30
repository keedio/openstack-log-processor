package com.keedio.flink.mappers

import java.sql.Timestamp

import com.keedio.flink.entities.LogEntry
import com.keedio.flink.utils.{ProcessorHelper, SyslogCode}
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.java.tuple.Tuple7
import org.apache.flink.configuration.Configuration
import org.apache.flink.metrics.Counter
import org.joda.time.DateTime

/**
  * Created by luislazaro on 24/3/17.
  * lalazaro@keedio.com
  * Keedio
  */
class RichMapFunctionRL(val region: String) extends RichMapFunction[LogEntry, Tuple7[String, String, String, String, String, Timestamp, String]] {
  var counter: Counter = _

  @throws(classOf[Exception])
  override def map(logEntry: LogEntry): Tuple7[String, String, String, String, String, Timestamp, String] = {
    this.counter.inc()
    val logLevel: String = SyslogCode.severity(logEntry.severity)
    val service = logEntry.service
    val node_type = logEntry.hostname
    val timestamp: DateTime = new DateTime(ProcessorHelper.toTimestamp(logEntry.timestamp))
    val pieceDate: String = new String(timestamp.getYear.toString + "-" + timestamp.getMonthOfYear.toString + "-" +
      timestamp.getDayOfMonth.toString)
    val log_ts: Timestamp = ProcessorHelper.toTimestamp(logEntry.timestamp)
    new Tuple7(pieceDate, region, logLevel, service, node_type, log_ts, logEntry.body)
  }

  override def open(configuration: Configuration) = {
    this.counter = getRuntimeContext()
      .getMetricGroup
      .counter("raw_log_LogEntryToTuple")
  }
}
