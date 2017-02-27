package com.keedio.flink.utils

import org.apache.log4j.Logger
import org.joda.time._

/**
  * Created by luislazaro on 27/2/17.
  * lalazaro@keedio.com
  * Keedio
  */
object ProcessorHelper {
  val LOG: Logger = Logger.getLogger(getClass)

  /**
    * The intention is to validate the log printed hour against the hour range they should belong to.
    * Specifically, if the hour range is  {1h, 6h, 12h, 24h, 1w, 1m}, each of this values but transformated into
    * seconds units would be the valkey and the “timeframe” then would be the log hour per 60 mins.
    * One log hour would be valid only if the timeframe between the log hour and current hour is whihin the framework that the “timeframe” field limits
    * For example:
    *   -    Current hour is 15:00 and the log is 08:00. Time difference would be 15-8=7 hours, this will render
    * that all hour values would be acceptable but 1h and 6 h since this ones are above the range
    *   -    If current hour is 15:00 and log is 17:00, time difference would be then 24-17+15=22 hours
    * in this case the acceptable values would be any but 1h, 6 h and 12h since these ones are abobe the range
    *
    * @param timeframe
    * @param valKey
    * @return
    */
  def isValidTimeFrame(timeframe: Int, valKey: Int, now: DateTime = DateTime.now()): Boolean = {
    val timeframeSeconds: Int = timeframe * 60
    val nowSeconds: Int = now.getHourOfDay * 3600 + now.getMinuteOfHour * 60 + now.getSecondOfMinute
    timeframeSeconds <= nowSeconds match {
      case true => (nowSeconds - timeframeSeconds) <= valKey
      case false => (24 * 60 * 60) - timeframeSeconds + nowSeconds <= valKey
    }
  }

  /**
    * TTL is computed.
    * @param timeframe
    * @param valKey
    * @param now
    * @return
    */
  def computeTTL(timeframe: Int, valKey: Int, now: DateTime = DateTime.now()): Int = {
    val timeframeSeconds: Int = timeframe * 60
    val nowSeconds: Int = now.getHourOfDay * 3600 + now.getMinuteOfHour * 60 + now.getSecondOfMinute
    timeframeSeconds <= nowSeconds match {
      case true => valKey - (nowSeconds - timeframeSeconds)
      case false => valKey - ((24 * 60 * 60) - timeframeSeconds + nowSeconds)
    }
  }

  /**
    * Get minutes from time token in syslog
    * 09:40 == 09*60 + 40
    *
    * @param pieceTime
    * @return
    */
  def getMinutesFromTimePieceLogLine(pieceTime: String): Int = {
    var pieceHour = 0
    var pieceMinute = 0
    try {
      pieceHour = pieceTime.split(":")(0).toInt * 60
      pieceMinute = pieceTime.split(":")(1).toInt
    } catch {
      case e: NumberFormatException => LOG.warn("String cannot be cast to Integer: " + pieceTime)
      case e: ArrayIndexOutOfBoundsException => LOG.warn("Malformed piece of time : " + pieceTime)
    }
    pieceHour + pieceMinute
  }

  /**
    * Extract value "log-level" form a common syslog line.
    * Value for log level info is expected to be the fourth word in a standarized syslog line.
    * If standard is not met, use argument 'exp' for marking off an expression.
    * Example:
    * - common syslog: "2017-02-10 06:18:07.264 3397 INFO eventlet.wsgi.server ...some other stuff"
    * - irregular syslog: "whatever myMachineName: 2017-02-10 06:18:07.264 3397 INFO eventlet.wsgi.server ...some other stuff"
    * exp = "myMachine:"
    *
    * @param s
    * @param exp
    * @return
    */
  def getFieldFromString(s: String, exp: String = "", position: Int): String = {
    var requiredValue: String = ""
    try {
      requiredValue = s.trim.split("\\s+")(position)
    } catch {
      case e: ArrayIndexOutOfBoundsException => LOG.error("Cannot parse string: does line contains loglevel info or timestamp? " + s)
    }
    requiredValue
  }

  /**
    * Auxiliar function for creation a Joda.Period object parsing the date and time
    * from string log.
    * Assumption: '2017-02-10 18:22:08.376' is always the first token from standard syslog.
    * @param string
    * @return
    */
  def buildDateTimeFromFieldsLog(string: String): DateTime = {
    val pieceDate = ProcessorHelper.getFieldFromString(string, "", 0).trim
    val year = pieceDate.split("-")(0).toInt
    val month = pieceDate.split("-")(1).toInt
    val day = pieceDate.split("-")(2).toInt
    val pieceTime: String = ProcessorHelper.getFieldFromString(string, "", 1).trim
    val hour = pieceTime.split(":")(0).toInt
    val minute = pieceTime.split(":")(1).toInt
    val seconds = pieceTime.split(":")(2).split("\\.")(0).toInt
    val mseconds = pieceTime.split(":")(2).split("\\.")(1).toInt
    new DateTime(year, month, day, hour, minute, seconds, mseconds)
  }

}
