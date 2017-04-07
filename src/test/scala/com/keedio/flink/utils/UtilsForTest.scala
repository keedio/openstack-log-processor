package com.keedio.flink.utils

import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat

import scala.collection.immutable.Seq

/**
  * Created by luislazaro on 4/4/17.
  * lalazaro@keedio.com
  * Keedio
  */
object UtilsForTest {
  /**
    * Auxiliar function for generating a list of strings of timestamps from diferents
    * periods of time before now().
    *
    * @return
    */
  def generateTimestamps(): Seq[String] = {
    val now: DateTime = DateTime.now
    val fmt = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.SSS")
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
    listOfTimes.map(timestamp => {
      val logLevel: String = SyslogCode.severity.get(scala.util.Random.nextInt(7).toString).get
      val bodyField = s"whatever - - - ${timestamp} 0123456 ${logLevel} whatever.whatever [req-3a832c6b-c"
      val severityField = scala.util.Random.nextInt(7).toString
      val serviceField = ProcessorHelperPoc.generateRandomService
      val id = new String(DateTime.now.getMillis + "_" + timestamp + "_" + timestamp.hashCode())
      new String(
        s"""{\"severity\":\"$severityField\",\"body\":\"$bodyField\",\"spriority\":\"13\",
           \"hostname\":\"poc-rhlogs\",\"protocol\":\"UDP\",\"port\":\"7780\",\"sender\":\"/192.168.0.2\",
           \"service\":\"$serviceField\",\"id\":\"$id\",
           \"facility\":\"1\",\"timestamp\":\"$timestamp\"}""".stripLineEnd)
    })
  }
}
