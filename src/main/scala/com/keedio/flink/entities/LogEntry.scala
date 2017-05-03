package com.keedio.flink.entities

import java.io.Serializable
import java.sql.Timestamp

import com.keedio.flink.utils.SyslogCode
import org.apache.log4j.Logger
import org.json4s.DefaultFormats
import org.json4s.native.JsonMethods._

import scala.util._
import scala.util.matching.Regex

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
                val port: String,
                val sender: String,
                val service: String,
                val id: String,
                val facility: String,
                val timestamp: String){

  def this() = this("", "", "", "", "", "", "", "", "", "", "")

  def isValid() = {
    !timestamp.isEmpty && !severity.isEmpty && !body.isEmpty
  }

  //override def toString = s"$timestamp, $hostname, $severity, $protocol, $port, $sender, $service, $id, $facility, $body"
  override def toString = new String(
    s"""{\"severity\":\"$severity\",\"body\":\"$body\",\"spriority\":\"$spriority\",\"hostname\":\"$hostname\",\"protocol\":\"$protocol\",\"port\":\"$port\",\"sender\":\"$sender",\"service\":\"$service\",\"id\":\"$id\",\"facility\":\"$facility\",\"timestamp\":\"$timestamp\"}""")

  def canEqual(other: Any): Boolean = other.isInstanceOf[LogEntry]

  override def equals(other: Any): Boolean = other match {
    case that: LogEntry =>
      (that canEqual this) &&
        severity == that.severity &&
        body == that.body &&
        spriority == that.spriority &&
        hostname == that.hostname &&
        protocol == that.protocol &&
        port == that.port &&
        sender == that.sender &&
        service == that.service &&
        id == that.id &&
        facility == that.facility &&
        timestamp == that.timestamp
    case _ => false
  }

  override def hashCode(): Int = {
    val state = Seq(severity, body, spriority, hostname, protocol, port, sender, service, id, facility, timestamp)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }
}

object LogEntry extends Serializable {

  val LOG: Logger = Logger.getLogger(classOf[LogEntry])

  def apply(s: String, parseBody: Boolean = true): LogEntry = {
    implicit val formats = DefaultFormats
    val parsedlog = Try(parse(s).extract[LogEntry])
    parsedlog.isSuccess match {
      case true => parseBody match {
        case true => getInfoFromBody(parsedlog.get)
        case false => parsedlog.get
      }
      case false =>
        LOG.error("cannot parse string to LogEntry: " + s)
        new LogEntry()
    }
  }

  def getFields(): Seq[String] = {
    getClass.getDeclaredFields.map(_.getName).toSeq
  }

  /**
    * We may need to get serveral attributes from payload's body.
    *
    * @param logEntry
    * @return
    */
  def getInfoFromBody(logEntry: LogEntry): LogEntry = {
    val payload: String = logEntry.body
    val timestampFromBody: String = getTimestampFromBody(payload)
    val severityFromBody: String = getSeverityFromBody(payload)
    val infoFromBody: String = getInfoFromBody(payload)
    new LogEntry(severityFromBody, infoFromBody, logEntry.spriority, logEntry.hostname, logEntry.protocol, logEntry.port,
      logEntry.sender, logEntry.service, logEntry.id, logEntry.facility, timestampFromBody)
  }

  /**
    * extract a string that matches a timestamp
    *
    * @param payload
    * @return
    */
  def getTimestampFromBody(payload: String): String = {
    val regexForTimestamp: Regex = "[0-9]{4}\\-[0-9]{2}\\-[0-9]{2}\\s[0-9]{2}\\:[0-9]{2}\\:[0-9]{2}\\.[0-9]{3}".r
    regexForTimestamp.findFirstIn(payload) match {
      case Some(timestampFromTheBody) => timestampFromTheBody
      case None =>
        LOG.error(s"cannot parse timestamp with regex ${regexForTimestamp} from body: ${payload}")
        new Timestamp(0L).toString
    }
  }

  /**
    * extract string that matches a severity attribute
    * If severity is extracted, for example "INFO", we must
    * retunr number of INFO severity.
    *
    * @param payload
    * @return
    */
  def getSeverityFromBody(payload: String): String = {
    val regexForSeverity: Regex = "[A-Z]{4,7}+".r
    regexForSeverity.findFirstIn(payload) match {
      case Some(severityFromBody) => SyslogCode.severity.values.toSeq.contains(severityFromBody) match {
        case true => SyslogCode.numberOfSeverity.get(severityFromBody).get
        case false =>
          LOG.error(s"cannot parse severity(log-level), ${severityFromBody} (matched this expression)" +
            s" is not contanined in list of SyslogCode. Body was: ${payload}")
          ""
      }
      case None =>
        LOG.error(s"cannot parse severity(log-level) with regex ${regexForSeverity} from body: ${payload}")
        ""
    }
  }

  /**
    * extract body (real payload) from payload
    * example: syslog.body: "....warning...[-] description about what happened"
    * real body is "[-] description about what happened "
    *
    * @param payload
    * @return
    */
  def getInfoFromBody(payload: String): String = {
    val regexForSubBody: Regex = "\\[.*".r
    regexForSubBody.findFirstIn(payload) match {
      case Some(infoFromBody) => infoFromBody
      case None =>
        LOG.error(s"cannot parse info with regex ${regexForSubBody} from body: ${payload}")
        ""
    }
  }


}



