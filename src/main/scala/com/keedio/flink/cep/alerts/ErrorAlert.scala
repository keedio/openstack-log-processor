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
class ErrorAlert(val logEntry0: LogEntry, val logEntry1: LogEntry) extends IAlert{

  override def toString: String = {
   "ErrorAlert: " + logEntry0 + "/--->" +  logEntry1
  }
  def getEventSummary(logEntry: LogEntry): String = {
    logEntry.toString
  }

}
