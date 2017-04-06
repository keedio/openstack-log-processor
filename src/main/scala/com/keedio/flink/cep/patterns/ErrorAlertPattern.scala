package com.keedio.flink.cep.patterns

import com.keedio.flink.cep.IAlertPattern
import com.keedio.flink.cep.alerts.ErrorAlert
import com.keedio.flink.entities.LogEntry
import com.keedio.flink.utils.SyslogCode
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.windowing.time.Time

/**
  * Created by luislazaro on 31/3/17.
  * lalazaro@keedio.com
  * Keedio
  */
class ErrorAlertPattern extends IAlertPattern[LogEntry, ErrorAlert] {

  override def create(pattern: java.util.Map[String, LogEntry]): ErrorAlert = {
    val first: LogEntry = pattern.get("First Event")
    val second: LogEntry = pattern.get("Second Event")
    new ErrorAlert(first, second)
  }

  override def getEventPattern(): Pattern[LogEntry, _] = {
    Pattern
      .begin[LogEntry]("First Event")
      .subtype(classOf[LogEntry])
      .where(event => event.severity == SyslogCode.numberOfSeverity("ERROR"))
      .next("Second Event")
      .subtype(classOf[LogEntry])
      .where(event => event.severity == SyslogCode.numberOfSeverity("ERROR"))
      .within(Time.minutes(10))
  }

  override def getAlertTargetType(): Class[ErrorAlert] = {
    classOf[ErrorAlert]
  }


}
