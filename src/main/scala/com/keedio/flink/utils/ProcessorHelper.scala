package com.keedio.flink.utils

import java.sql.Timestamp
import java.util.Date

import org.apache.commons.lang3.time.DateUtils
import org.apache.log4j.Logger
import org.joda.time._
import org.joda.time.format.{DateTimeFormat, DateTimeFormatterBuilder}

import scala.collection.immutable.Seq

/**
  * Created by luislazaro on 27/2/17.
  * lalazaro@keedio.com
  * Keedio
  */
object ProcessorHelper {
  val LOG: Logger = Logger.getLogger(getClass)

  /**
    * Validate date + time from timestamp attribute
    * * The intention is to validate the log printed hour against the hour range they should belong to.
    * Specifically, if the hour range is  {1h, 6h, 12h, 24h, 1w, 1m}, each of this values but transformated into
    * seconds units would be the valkey and the “timeframe” then would be the log hour per 60 mins.
    * One log hour would be valid only if the timeframe between the log hour and current hour is whihin the framework
    * that the “timeframe” field limits
    * @param timestamp
    * @param valKey
    * @param now
    * @return
    */
  def isValidPeriodTime(timestamp: String, valKey: Int, now: DateTime = DateTime.now()): Boolean = {
    val dateTimeFromLog: DateTime = getParsedTimestamp(timestamp)
    Seconds.secondsBetween(dateTimeFromLog, now).getSeconds <= valKey
  }

  /**
    * TTL is computed on basis timestamp
    *
    * @param timestamp
    * @param valKey
    * @param now
    * @return
    */
  def computeTTLperiod(timestamp: String, valKey: Int, now: DateTime = DateTime.now()): Int = {
    val dateTimeFromLog: DateTime = getParsedTimestamp(timestamp)
    valKey - Seconds.secondsBetween(dateTimeFromLog, now).getSeconds
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
    * Generate a list of strings of timestamps from diferents
    * periods of time before now().
    *
    * @return
    */
  def generateTimestamps(): Seq[String] = {
    val now: DateTime = DateTime.now
    val fmt = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.SSS")
    //val fmt1 = DateTimeFormat.forPattern("MMM dd yyyy HH:mm:ss")
    val listMinutes: Seq[String] = (for (i <- 1 to 60) yield now.minusMinutes(i)) map (fmt.print(_))
    val listHours = (for (i <- 1 to 24) yield now.minusHours(i)) map (fmt.print(_))
    val listDays = (for (i <- 1 to 30) yield now.minusDays(i)) map (fmt.print(_))
    val listWeeks = (for (i <- 1 to 24) yield now.minusWeeks(i)) map (fmt.print(_))
    val listMonths = (for (i <- 1 to 6) yield now.minusMonths(i)) map (fmt.print(_))
    val listMinutes2: Seq[String] = (for (i <- 1 to 60) yield now.minusMinutes(i)) map (fmt.print(_))
    (listMinutes ++ listHours ++ listDays ++ listWeeks ++ listMonths ++ listMinutes2 ++ Nil)
  }

  /**
    * Create a list of Json supplying severals fields for
    *
    * @param listOfTimes
    * @return
    */
  def generateListOflogs(listOfTimes: Seq[String]) = {
    val bodyField = "root: payload of information .....[]"
    listOfTimes.map(timestamp => {
      val severityField = scala.util.Random.nextInt(7).toString
      val serviceField = ProcessorHelperPoc.generateRandomService
      new String(
        s"""{\"severity\":\"$severityField\",\"body\":\"$bodyField\",\"spriority\":\"13\",
           \"hostname\":\"poc-rhlogs\",\"protocol\":\"UDP\",\"port\":\"7780\",\"sender\":\"/192.168.0.2\",
           \"service\":\"$serviceField\",\"id\":\"5143170000_8c3dbd91-410e-4410-9d36-dfa4989df1ab\",
           \"facility\":\"1\",\"timestamp\":\"$timestamp\"}""".stripLineEnd)
    })
  }

  /**
    * Function to parse string as Joda-DateTime. String are matched
    * against list of patterns via DateTimeFormatterBuilder
    *
    * @param timestamp
    * @return
    */
  def getParsedTimestamp(timestamp: String): DateTime = {
    val listOfPatterns = Array(
      "yyyy-MM-dd HH:mm:ss.SSS",
      "yyyy-MM-dd HH:mm:ss",
      "MMM dd yyyy HH:mm:ss",
      "yyyy/MM/dd HH:mm:ss.SSS"
    )
    val parsers = listOfPatterns.map(DateTimeFormat.forPattern(_).getParser)
    val formatter = new DateTimeFormatterBuilder().append(null, parsers).toFormatter
    try {
      formatter.parseDateTime(timestamp)
    } catch {
      case e: Exception =>
        LOG.error(s"cannot parse provided timestamp $timestamp")
        new DateTime()
    }
  }

  /**
    * Obtener en minutos las horas y minutos
    * @param timestampAsString
    * @return
    */
  def getTimeFrameMinutes(timestampAsString: String) = {
    val parsedTime = ProcessorHelper.getParsedTimestamp(timestampAsString)
    parsedTime.getHourOfDay * 60 + parsedTime.getMinuteOfHour
  }

  /**
    * generate java.sql.Timestamp from string
    * @param dateAsString
    * @return
    */
  def toTimestamp(dateAsString: String): Timestamp = {
    val listOfFormats = Seq(
      "yyyy-MM-dd HH:mm:ss.SSS",
      "yyyy-MM-dd HH:mm:ss",
      "MMM dd yyyy HH:mm:ss",
      "yyyy/MM/dd HH:mm:ss.SSS"
    )
    try {
      val a: Date = DateUtils.parseDateStrictly(dateAsString, listOfFormats: _*)
      new Timestamp(a.getTime)
    } catch {
      case t: Throwable => {
        LOG.error("Supplying Timestamp with Epoch Time.", t)
        new Timestamp(0L)
      }
    }


  }

}
