package com.keedio.flink.entities

/**
  * Created by luislazaro on 16/2/17.
  * lalazaro@keedio.com
  * Keedio
  */
class LogEntry(lineOfLog: String, fields: Seq[String], separator: String) extends Serializable {

  private val fieldsOfLineLog: Seq[String] = getValuesFromLineOfLog(lineOfLog, separator)
  val valuesMap: Map[String, String] = fillValuesMap(fields, fieldsOfLineLog)

  /**
    * Build a list of strings from a line of syslog
    *
    * @param lineOfLog
    * @param separator
    * @return
    */
  def getValuesFromLineOfLog(lineOfLog: String, separator: String): Seq[String] = {
    lineOfLog.split(separator).map(_.trim) toSeq
  }

  /**
    * Filling elements of map zipping keys with values.
    * If number of elements are equals (k -> v)
    * If number of fields < values rest of values from line log become a single value called remainingLong
    *
    * @param fields
    * @param fieldsOfLineLog
    * @return
    */
  def fillValuesMap(fields: Seq[String], fieldsOfLineLog: Seq[String]): Map[String, String] = {
    if (fields.size == fieldsOfLineLog.size) {
      fields zip fieldsOfLineLog toMap
    } else if (fields.size < fieldsOfLineLog.size) {
      val mapAux = fields zip fieldsOfLineLog toMap
      val remainingLog = "remainingLog"
      val lastValues: Seq[String] = fieldsOfLineLog.slice(fields.size,  fieldsOfLineLog.size)
      (fields zip fieldsOfLineLog toMap) + (remainingLog -> lastValues.mkString(separator))
    } else fields zip fieldsOfLineLog toMap
  }


}
