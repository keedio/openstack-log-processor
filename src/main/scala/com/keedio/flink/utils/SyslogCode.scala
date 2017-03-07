package com.keedio.flink.utils

/**
  * Created by luislazaro on 1/3/17.
  * lalazaro@keedio.com
  * Keedio
  */
object SyslogCode {

  def apply(s: String) = {
    require(severity.unzip._1.toSeq.contains(s))
    severity.getOrElse(s, "Not found value of SyslogCode for supplied key")
  }

  val severity = Map(
    "0" -> "emerg",
    "1" -> "alert",
    "2" -> "crit",
    "3" -> "ERROR",
    "4" -> "WARNING",
    "5" -> "notice",
    "6" -> "INFO",
    "7" -> "debug"
  )
  val acceptedLogLevels = Seq("ERROR", "WARNING", "INFO")
}
