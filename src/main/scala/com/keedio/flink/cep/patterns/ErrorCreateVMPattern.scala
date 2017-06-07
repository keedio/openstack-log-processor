package com.keedio.flink.cep.patterns

import com.keedio.flink.cep.IPattern
import com.keedio.flink.cep.alerts.Alert
import com.keedio.flink.entities.LogEntry
import com.keedio.flink.utils.SyslogCode
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.windowing.time.Time

/**
  * Created by luislazaro on 26/4/17.
  * lalazaro@keedio.com
  * Keedio
  */
class ErrorCreateVMPattern extends IPattern[LogEntry, Alert] {

  override def createAlert(pattern: java.util.Map[String, java.util.List[LogEntry]]): Alert = {
    val first: LogEntry = pattern.get("First Event").get(0)
    val second: LogEntry = pattern.get("Second Event").get(0)
    new Alert(first, second)
  }

  /**
    * Genereate an Alert if and only if it matches two consecutive LogEntries for the same host
    * withi severity ERROR
    *
    * @return
    */
  override def getEventPattern(): Pattern[LogEntry, _] = {
    Pattern
      .begin[LogEntry]("First Event")
      .subtype(classOf[LogEntry])
      .where(event => event.severity == SyslogCode.numberOfSeverity("ERROR"))
      .where(event => event.service == "Nova")
      .where(event => event.body.contains("CEP_ID"))
      .followedBy("Second Event")
      .subtype(classOf[LogEntry])
      .where(event => event.severity == SyslogCode.numberOfSeverity("INFO"))
      .where(event => event.service == "Neutron")
      .where(event => event.body.contains("CEP_ID"))
      .where(
        (event, ctx) => {
          val matches: Seq[LogEntry] = ctx.getEventsForPattern("First Event").toSeq.filter(_.body.contains("CEP_ID="))
          matches.exists(logEntry => logEntry.body.split("CEP_ID=")(1).split("\\s+")(0) == event.body.split("CEP_ID=")(1).split("\\s+")(0))
        }
      )
      .within(Time.minutes(10))
  }


}
