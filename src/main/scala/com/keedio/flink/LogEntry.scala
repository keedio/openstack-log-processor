package com.keedio.flink

import scala.collection.Map

/**
  * Created by luislazaro on 16/2/17.
  * lalazaro@keedio.com
  * Keedio
  */
class LogEntry(lineOfLog: String, fields: Seq[String]) extends Serializable {

  val valuesMap: Map[String, String] = fields.zip(getFieldsFromLineOfLog(lineOfLog, separator = "\\s+")).toMap
  private final val date = "date"
  private final val time = "time"
  private final val logLevel = "logLevel"
  private final val pid = "pid"

  /**
    * Build a list of strings from a line of syslog
    * @param lineOfLog
    * @param separator
    * @return
    */
  def getFieldsFromLineOfLog(lineOfLog: String, separator: String): Seq[String] = {
    lineOfLog.split(separator) toSeq
  }

}
