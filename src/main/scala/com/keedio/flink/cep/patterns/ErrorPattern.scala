package com.keedio.flink.cep.patterns

import com.keedio.flink.cep.IPattern
import com.keedio.flink.cep.alerts.Alert
import com.keedio.flink.entities.LogEntry
import com.keedio.flink.utils.SyslogCode
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.windowing.time.Time

/**
  * Created by luislazaro on 31/3/17.
  * lalazaro@keedio.com
  * Keedio
  */
class ErrorPattern extends IPattern[LogEntry, Alert] {

  override def createAlert(pattern: java.util.Map[String, java.util.List[LogEntry]]): Alert = {
    val first: LogEntry = pattern.get("First Event").get(0)
    val second: LogEntry = pattern.get("Second Event").get(0)
    new Alert(first, second)
  }

  /**
    * Genereate an Alert if and only if it matches two consecutive LogEntries for the same service wHich severity is
    * ERROR and Logentries have to occur within a time interval of 10 minutes.
    * @return
    */
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
}
