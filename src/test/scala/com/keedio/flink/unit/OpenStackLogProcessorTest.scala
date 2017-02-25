package com.keedio.flink.unit

import java.sql.Timestamp
import java.util.concurrent.TimeUnit

import com.keedio.flink.{DbTable, LogEntry, OpenStackLogProcessor}
import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}
import org.apache.flink.api.scala.createTypeInformation
import org.joda.time.DateTime
import org.junit.{Assert, Test}


/**
  * Created by luislazaro on 14/2/17.
  * lalazaro@keedio.com
  * Keedio
  */

class OpenStackLogProcessorTest {

  @Test
  def testgetLogLevelFromString() = {
    val lineOfLog: String = "2017-02-10 06:18:07.264 3397 INFO eventlet.wsgi.server [req-08ef6dd2-4f3b-44ae-8d16-992adcc009ef" +
      " acab852ba0b3489185d19ade26914272 ed757fde810048e7b798d984e9dfeb49 - - -] 192.168.0.20 - - " +
      "[10/Feb/2017 06:18:07] \"GET /v1/images/detail?is_public=None&limit=20 HTTP/1.1\" 200 2862 0.290697"
    Assert.assertTrue(OpenStackLogProcessor.getFieldFromString(lineOfLog, "", 3) == "INFO")
  }

  @Test
  def testTimestampToTimeFrame() = {
    val lineOfLog: String = "2017-02-10 06:18:07.264 3397 INFO eventlet.wsgi.server [req-08ef6dd2-4f3b-44ae-8d16-992adcc009ef" +
      " acab852ba0b3489185d19ade26914272 ed757fde810048e7b798d984e9dfeb49 - - -] 192.168.0.20 - - " +
      "[10/Feb/2017 06:18:07] \"GET /v1/images/detail?is_public=None&limit=20 HTTP/1.1\" 200 2862 0.290697"
    val stringtimestamp: String = new String(OpenStackLogProcessor.getFieldFromString(lineOfLog, "", 0)
      + " " + OpenStackLogProcessor.getFieldFromString(lineOfLog, "", 1))
    Assert.assertEquals(stringtimestamp, "2017-02-10 06:18:07.264")
    val millis: Long = Timestamp.valueOf(stringtimestamp).getTime
    val minutes: Long = TimeUnit.MILLISECONDS.toMinutes(millis)
    Assert.assertEquals(minutes, 24778398L)

  }

  @Test
  def testTimeFrameInMinutes() = {
    val lineOfLog = "2016-03-29 07:58:09.232 2535 INFO eventlet.wsgi.server [req-d34145b2-f2bb-4cdc-9399-94a2bdc4f67c " +
      "acab852ba0b3489185d19ade26914272 ed757fde810048e7b798d984e9dfeb49 - - -] 192.168.0.20 - - [29/Mar/2016 07:58:09] " +
      "HEAD /v1/images/fb263421-65d9-4d7d-bf87-d431eaf624d8 HTTP/1.1 200 1257 0.443069"
    val pieceTime: String = OpenStackLogProcessor.getFieldFromString(lineOfLog, "", 1)
    Assert.assertEquals(OpenStackLogProcessor.getMinutesFromTimePieceLogLine(pieceTime), (7 * 60 + 58))
  }

  @Test
  def testUseOfLogEntry() = {
    val lineOfLog = "2017-02-09 11:34:54.275 8348 INFO keystone.common.wsgi [-] POST http://192.168.0.20:5000/v2.0/tokens"
    val fields = Seq("date", "time", "pid", "loglevel", "service")
    val logEntry = new LogEntry(lineOfLog, fields, " ")
    Assert.assertEquals(logEntry.valuesMap("date"), "2017-02-09")
    Assert.assertEquals(logEntry.valuesMap("time"), "11:34:54.275")
    Assert.assertEquals(logEntry.valuesMap("service"), "keystone.common.wsgi")
    Assert.assertEquals(logEntry.valuesMap("remainingLog"), "[-] POST http://192.168.0.20:5000/v2.0/tokens")
  }

  @Test
  def testUserOfreadCsvFileFromBatchEnvironment() = {
    val envBatch = ExecutionEnvironment.getExecutionEnvironment
    val tablesLoaded: DataSet[String] = envBatch.readTextFile("./src/main/resources/tables/tables.csv")
    val datasetTables: DataSet[DbTable] = tablesLoaded.map(s => new DbTable(s.split(";")(0), s.split(";").slice(1, s.size - 1): _*))
    val a = datasetTables.map(Assert.assertNotNull(_))
  }

  @Test
  def testUseOfReadFileOfPrimitives() = {
    val envBatch = ExecutionEnvironment.getExecutionEnvironment
    val b: DataSet[_ <: Any] = envBatch.readFileOfPrimitives[String]("./src/main/resources/types/types")
  }

  /**
    * Se trata de validar la hora impresa en log frente a un rango de horas al que debe pertencer
    * el timeframe es la hora del log multiplicada por 60 minutos.
    */
  @Test
  def testForValidityTimeFrame() = {
    val valKeys = Seq(3600, 21600, 43200, 86400, 604800, 2419200)

    val eightOclockMorning: DateTime = new DateTime(2017, 2, 22, 8, 0)
    val timeFrameUpper_eight = 21 * 60  + 20
    val timeFrameLower_eight = 3 * 60 + 59
    val timeFrameMiddle_eight = 8 * 60 + 0

    //now is 08:00 and log is 21:00 belongs to {12h, 24h, 1m, 1w}
    valKeys.foreach(valkey => valkey match {
      case 3600 => Assert.assertFalse(OpenStackLogProcessor.isValidTimeFrame(timeFrameUpper_eight, 3600, eightOclockMorning))
      case 21600 => Assert.assertFalse(OpenStackLogProcessor.isValidTimeFrame(timeFrameUpper_eight, 21600, eightOclockMorning))
      case 43200 => Assert.assertTrue(OpenStackLogProcessor.isValidTimeFrame(timeFrameUpper_eight, 43200, eightOclockMorning))
      case 86400 => Assert.assertTrue(OpenStackLogProcessor.isValidTimeFrame(timeFrameUpper_eight, 86400, eightOclockMorning))
      case 604800 => Assert.assertTrue(OpenStackLogProcessor.isValidTimeFrame(timeFrameUpper_eight, 604800, eightOclockMorning))
      case 2419200 => Assert.assertTrue(OpenStackLogProcessor.isValidTimeFrame(timeFrameUpper_eight, 2412900, eightOclockMorning))
    })

    //now is 08:00 and log is 03:00 belongs to {6h, 12h, 24h, 1w, 1m}
    valKeys.foreach(valkey => valkey match {
      case 3600 => Assert.assertFalse(OpenStackLogProcessor.isValidTimeFrame(timeFrameLower_eight, 3600, eightOclockMorning))
      case 21600 => Assert.assertTrue(OpenStackLogProcessor.isValidTimeFrame(timeFrameLower_eight, 21600, eightOclockMorning))
      case 43200 => Assert.assertTrue(OpenStackLogProcessor.isValidTimeFrame(timeFrameLower_eight, 43200, eightOclockMorning))
      case 86400 => Assert.assertTrue(OpenStackLogProcessor.isValidTimeFrame(timeFrameLower_eight, 86400, eightOclockMorning))
      case 604800 => Assert.assertTrue(OpenStackLogProcessor.isValidTimeFrame(timeFrameLower_eight, 604800, eightOclockMorning))
      case 2419200 => Assert.assertTrue(OpenStackLogProcessor.isValidTimeFrame(timeFrameLower_eight, 2412900, eightOclockMorning))
    })

    //now is 08:00 and log is 08:00 belongs to all ranges
    valKeys.foreach(valkey => valkey match {
      case 3600 => Assert.assertTrue(OpenStackLogProcessor.isValidTimeFrame(timeFrameMiddle_eight, 3600, eightOclockMorning))
      case 21600 => Assert.assertTrue(OpenStackLogProcessor.isValidTimeFrame(timeFrameMiddle_eight, 21600, eightOclockMorning))
      case 43200 => Assert.assertTrue(OpenStackLogProcessor.isValidTimeFrame(timeFrameMiddle_eight, 43200, eightOclockMorning))
      case 86400 => Assert.assertTrue(OpenStackLogProcessor.isValidTimeFrame(timeFrameMiddle_eight, 86400, eightOclockMorning))
      case 604800 => Assert.assertTrue(OpenStackLogProcessor.isValidTimeFrame(timeFrameMiddle_eight, 604800, eightOclockMorning))
      case 2419200 => Assert.assertTrue(OpenStackLogProcessor.isValidTimeFrame(timeFrameMiddle_eight, 2412900, eightOclockMorning))
    })


    val sevenOclockEvening: DateTime = new DateTime(2017, 2, 22, 19, 0)
    val timeFrameUpper_seven = 21 * 60 + 59
    val timeFrameLower_seven = 3 * 60 + 1
    val timeFrameMiddle_seven = 19 * 60 + 0

    //now is 19:00 and log is 21:00 belongs to {24h, 1w, 1m}
    valKeys.foreach(valkey => valkey match {
      case 3600 => Assert.assertFalse(OpenStackLogProcessor.isValidTimeFrame(timeFrameUpper_seven, 3600, sevenOclockEvening))
      case 21600 => Assert.assertFalse(OpenStackLogProcessor.isValidTimeFrame(timeFrameUpper_seven, 21600, sevenOclockEvening))
      case 43200 => Assert.assertFalse(OpenStackLogProcessor.isValidTimeFrame(timeFrameUpper_seven, 43200, sevenOclockEvening))
      case 86400 => Assert.assertTrue(OpenStackLogProcessor.isValidTimeFrame(timeFrameUpper_seven, 86400, sevenOclockEvening))
      case 604800 => Assert.assertTrue(OpenStackLogProcessor.isValidTimeFrame(timeFrameUpper_seven, 604800, sevenOclockEvening))
      case 2419200 => Assert.assertTrue(OpenStackLogProcessor.isValidTimeFrame(timeFrameUpper_seven, 2412900, sevenOclockEvening))
    })

    //now is 19:00 and log is 03:00 belongs to {24h, 1w, 1m}
    valKeys.foreach(valkey => valkey match {
      case 3600 => Assert.assertFalse(OpenStackLogProcessor.isValidTimeFrame(timeFrameLower_seven, 3600, sevenOclockEvening))
      case 21600 => Assert.assertFalse(OpenStackLogProcessor.isValidTimeFrame(timeFrameLower_seven, 21600, sevenOclockEvening))
      case 43200 => Assert.assertFalse(OpenStackLogProcessor.isValidTimeFrame(timeFrameLower_seven, 43200, sevenOclockEvening))
      case 86400 => Assert.assertTrue(OpenStackLogProcessor.isValidTimeFrame(timeFrameLower_seven, 86400, sevenOclockEvening))
      case 604800 => Assert.assertTrue(OpenStackLogProcessor.isValidTimeFrame(timeFrameLower_seven, 604800, sevenOclockEvening))
      case 2419200 => Assert.assertTrue(OpenStackLogProcessor.isValidTimeFrame(timeFrameLower_seven, 2412900, sevenOclockEvening))
    })

    //now is 19:00 and log is 19:00 belongs to  all ranges
    valKeys.foreach(valkey => valkey match {
      case 3600 => Assert.assertTrue(OpenStackLogProcessor.isValidTimeFrame(timeFrameMiddle_seven, 3600, sevenOclockEvening))
      case 21600 => Assert.assertTrue(OpenStackLogProcessor.isValidTimeFrame(timeFrameMiddle_seven, 21600, sevenOclockEvening))
      case 43200 => Assert.assertTrue(OpenStackLogProcessor.isValidTimeFrame(timeFrameMiddle_seven, 43200, sevenOclockEvening))
      case 86400 => Assert.assertTrue(OpenStackLogProcessor.isValidTimeFrame(timeFrameMiddle_seven, 86400, sevenOclockEvening))
      case 604800 => Assert.assertTrue(OpenStackLogProcessor.isValidTimeFrame(timeFrameMiddle_seven, 604800, sevenOclockEvening))
      case 2419200 => Assert.assertTrue(OpenStackLogProcessor.isValidTimeFrame(timeFrameMiddle_seven, 2412900, sevenOclockEvening))
    })
  }

}