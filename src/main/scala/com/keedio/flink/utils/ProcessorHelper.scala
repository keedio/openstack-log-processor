package com.keedio.flink.utils

import java.sql.Timestamp
import java.util.Date

import org.apache.commons.lang3.time.DateUtils
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.log4j.Logger
import org.joda.time._

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
    *
    * @param timestamp
    * @param valKey
    * @param now
    * @return
    */
  def isValidPeriodTime(timestamp: String, valKey: Int, now: DateTime = DateTime.now()): Boolean = {
    val dateTimeFromLog: DateTime = new DateTime(ProcessorHelper.toTimestamp(timestamp))
    Seconds.secondsBetween(dateTimeFromLog, now).getSeconds < 0 match {
      case true =>
        LOG.error(s"Invalid syslog.timestamp,  $dateTimeFromLog from log later than now $now")
        false
      case false => Seconds.secondsBetween(dateTimeFromLog, now).getSeconds <= valKey
    }
  }

  /**
    * TTL is computed on basis timestamp
    *
    * @param timestamp
    * @param valKey
    * @param now
    * @return
    */
  def computeTTL(timestamp: String, valKey: Int, now: DateTime = DateTime.now()): Int = {
    val dateTimeLog = new DateTime(ProcessorHelper.toTimestamp(timestamp))
    valKey - Seconds.secondsBetween(dateTimeLog, now).getSeconds
  }


  /**
    * Obtener en minutos las horas y minutos
    *
    * @param timestamp
    * @return
    */
  def getTimeFrameMinutes(timestamp: String) = {
    val parsedTime = new DateTime(ProcessorHelper.toTimestamp(timestamp))
    parsedTime.getHourOfDay * 60 + parsedTime.getMinuteOfHour
  }

  /**
    * generate java.sql.Timestamp from string
    *
    * @param dateAsString
    * @return
    */
  def toTimestamp(dateAsString: String): Timestamp = {
    val listOfFormats = Seq(
      "yyyy-MM-dd HH:mm:ss.SSS",
      "yyyy-MM-dd HH:mm:ss",
      "MMM dd yyyy HH:mm:ss",
      "yyyy/MM/dd HH:mm:ss.SSS",
      "yyyy-MM-dd'T'HH:mm:ss.SSSSSSZZ",
      "yyyy-MM-dd'T'HH:mm:ss.SSSSSSzz",
      "yyyy-MM-dd'T'HH:mm:ss.SSSSSS'+'SS:SS"
    )
    try {
      val a: Date = DateUtils.parseDateStrictly(dateAsString, listOfFormats: _*)
      new Timestamp(a.getTime)
    } catch {
      case t: Throwable => {
        LOG.error(s"Cannot parse syslog.timestamp \'$dateAsString\', supplying Timestamp with Epoch Time.", t)
        new Timestamp(0L)
      }
    }
  }

  /**
    * Check for not mandatory key in parametertool map. If not exists,  provide default value for such a key.
    * If key exists, check for value and return provided value or default.
    * Method for pairs key-vals in map parametertool.
    * @param parameterTool
    * @param key
    * @param default
    * @return
    */
  def getValueFromArgs(parameterTool: ParameterTool, key: String, default: String): String = {
    parameterTool.has(key) match {
      case true => Option(parameterTool.get(key)) match {
        case Some(value) => value match {
          case "" => default
          case _ => value
        }
        case None => default
      }
      case false => default
    }
  }

}
