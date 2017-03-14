package com.keedio.flink.entities

import org.json4s.DefaultFormats
import org.json4s.native.JsonMethods._
import org.apache.log4j.Logger

import scala.util._

/**
  * Created by luislazaro on 16/2/17.
  * lalazaro@keedio.com
  * Keedio
  */
class LogEntry(
                val severity: String,
                val body: String,
                val spriority: String,
                val hostname: String,
                val protocol: String,
                val port    : String,
                val sender  : String,
                val service : String,
                val id      : String,
                val facility: String,
                val timestamp: String) {

  def this() = this("", "", "", "", "", "", "", "", "", "", "")

  def isValid()= {
    ! timestamp.isEmpty && ! severity.isEmpty
  }

  override def toString = s"$timestamp, $hostname, $severity, $protocol, $port, $sender, $service, $id, $facility, $body"
}

object LogEntry extends Serializable {

  val LOG: Logger = Logger.getLogger(classOf[LogEntry])

  def apply(s  : String): LogEntry = {
    implicit val formats = DefaultFormats
    val parsedlog = Try(parse(s).extract[LogEntry])
      parsedlog.isSuccess match {
      case true => parsedlog.get
      case false =>
        LOG.error("cannot parse string to LogEntry: " + s)
        new LogEntry()
    }
  }
  def getFields(): Seq[String] = {
    getClass.getDeclaredFields.map(_.getName).toSeq
  }

}



