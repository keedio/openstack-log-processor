package com.keedio.flink.entities

import org.junit.{Assert, Test}
import org.json4s._
import org.json4s.native.JsonMethods._

/**
  * Created by luislazaro on 28/2/17.
  * lalazaro@keedio.com
  * Keedio
  */
class LogEntryTest {

  val valid_json_logEntry =
    """{"severity":"5","body":"root: 2016-02-15 07:51:03.086 2770 WARNING keystonemiddleware.auth_token
      |[req-65ba328e-bc31-4f14-9978-24ad821ae4db acab852ba0b3489185d19ade26914272 ed757fde810048e7b798d984e9dfeb49
      |- - -] Authorization failed for token","spriority":"13","hostname":"poc-rhlogs","protocol":"UDP",
      |"port":"7780","sender":"/192.168.0.2","service":"Keystone",
      |"id":"5141104000_849dc3a4-3061-444d-ae86-2f554a57fb13","facility":"1","timestamp":"2016-02-20 06:36:14.702"}"""
      .stripMargin

  val unparseable_Json_logEntry =
    """{"NOMBRE_DE_CAMPO_NO_ESPERADO":"5","body":"root: 2016-02-15 07:51:03.086 2770 WARNING keystonemiddleware
      |.auth_token
      |[req-65ba328e-bc31-4f14-9978-24ad821ae4db acab852ba0b3489185d19ade26914272 ed757fde810048e7b798d984e9dfeb49
      |- - -] Authorization failed for token","spriority":"13","hostname":"poc-rhlogs","protocol":"UDP",
      |"port":"7780","sender":"/192.168.0.2","service":"Keystone",
      |"id":"5141104000_849dc3a4-3061-444d-ae86-2f554a57fb13","facility":"1","timestamp":"Mar  1 12:05:04"}"""
      .stripMargin

  val parseable_withoutTimestamp_Json_logEntry =
      """{"severity":"5","body":"root: 2016-02-15 07:51:03.086 2770 WARNING keystonemiddleware.auth_token
        |[req-65ba328e-bc31-4f14-9978-24ad821ae4db acab852ba0b3489185d19ade26914272 ed757fde810048e7b798d984e9dfeb49
        |- - -] Authorization failed for token","spriority":"13","hostname":"poc-rhlogs","protocol":"UDP",
        |"port":"7780","sender":"/192.168.0.2","service":"Keystone",
        |"id":"5141104000_849dc3a4-3061-444d-ae86-2f554a57fb13","facility":"1","timestamp":""}"""
        .stripMargin

  val parseable_but_invalid_Json_logEntry =
    """{"severity":"","body":"root: 2016-02-15 07:51:03.086 2770 WARNING keystonemiddleware.auth_token
      |[req-65ba328e-bc31-4f14-9978-24ad821ae4db acab852ba0b3489185d19ade26914272 ed757fde810048e7b798d984e9dfeb49
      |- - -] Authorization failed for token","spriority":"13","hostname":"poc-rhlogs","protocol":"UDP",
      |"port":"7780","sender":"/192.168.0.2","service":"Keystone",
      |"id":"5141104000_849dc3a4-3061-444d-ae86-2f554a57fb13","facility":"1","timestamp":"Mar  1 12:05:04"}"""
      .stripMargin

  /**
    * @see (http://json4s.org)
    *
    */
  @Test
  def testParseJsonToLogEntryEntity() = {
    implicit val formats = DefaultFormats
    val logEntry: LogEntry = parse(valid_json_logEntry).extract[LogEntry]
    Assert.assertEquals(logEntry.severity, "5")
    Assert.assertEquals(logEntry.spriority, "13")
    Assert.assertEquals(logEntry.hostname, "poc-rhlogs")
    Assert.assertEquals(logEntry.protocol, "UDP")
    Assert.assertEquals(logEntry.port, "7780")
    Assert.assertEquals(logEntry.sender, "/192.168.0.2")
    Assert.assertEquals(logEntry.service, "Keystone")
    Assert.assertEquals(logEntry.timestamp, "2016-02-20 06:36:14.702")
  }

  @Test
  def testLogentryApply() = {
    Assert.assertTrue(LogEntry(valid_json_logEntry).isInstanceOf[LogEntry])
    Assert.assertEquals(LogEntry(valid_json_logEntry).severity, "5")
    Assert.assertEquals(LogEntry(valid_json_logEntry).spriority, "13")
    Assert.assertEquals(LogEntry(valid_json_logEntry).hostname, "poc-rhlogs")
    Assert.assertEquals(LogEntry(valid_json_logEntry).protocol, "UDP")
    Assert.assertEquals(LogEntry(valid_json_logEntry).port, "7780")
    Assert.assertEquals(LogEntry(valid_json_logEntry).sender, "/192.168.0.2")
    Assert.assertEquals(LogEntry(valid_json_logEntry).service, "Keystone")
  }

  @Test
  def testInvalidLogEntry() = {
    Assert.assertTrue(LogEntry(valid_json_logEntry).isValid())
    Assert.assertFalse(LogEntry(unparseable_Json_logEntry).isValid())
    Assert.assertFalse(LogEntry(parseable_withoutTimestamp_Json_logEntry).isValid())
    Assert.assertFalse(LogEntry(parseable_but_invalid_Json_logEntry).isValid())
  }

}
