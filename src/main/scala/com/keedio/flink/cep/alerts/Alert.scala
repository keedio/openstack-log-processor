package com.keedio.flink.cep.alerts

import com.keedio.flink.cep.IAlert
import com.keedio.flink.entities.LogEntry

/**
  * Created by luislazaro on 31/3/17.
  * lalazaro@keedio.com
  * Keedio
  */
/**
  * Alert HighErrorAlert should be triggered if we expect a number of logs
  * @param logEntry0
  * @param logEntry1
  */
class Alert(val logEntry0: LogEntry, val logEntry1: LogEntry) extends IAlert{

  override def toString: String = {
   "Alert: " + "\n" + logEntry0.service +"-"+ logEntry0.severity+"-"+logEntry0.timestamp  + "\n" +
     logEntry1.service +"-"+ logEntry1.severity+"-"+logEntry1.timestamp + "\n" +
    logEntry0 + "\n" + logEntry1 + "\n"
  }
  def getEventSummary(logEntry: LogEntry): String = {
    logEntry.toString
  }

  def getAlertName = "Alert"

  def canEqual(other: Any): Boolean = other.isInstanceOf[Alert]

  override def equals(other: Any): Boolean = other match {
    case that: Alert =>
      (that canEqual this) &&
        logEntry0 == that.logEntry0 &&
        logEntry1 == that.logEntry1
    case _ => false
  }

  override def hashCode(): Int = {
    val state = Seq(logEntry0, logEntry1)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }


}
