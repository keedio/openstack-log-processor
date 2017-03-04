package com.keedio.flink

import com.keedio.flink.entities.DbTable
import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment, createTypeInformation}
import org.junit.{Assert, Test}

/**
  * Created by luislazaro on 14/2/17.
  * lalazaro@keedio.com
  * Keedio
  */

class OpenStackLogProcessorTest {

 /* @Test
  def testgetLogLevelFromString() = {
    val lineOfLog: String = "2017-02-10 06:18:07.264 3397 INFO eventlet.wsgi.server " +
      "[req-08ef6dd2-4f3b-44ae-8d16-992adcc009ef" +
      " acab852ba0b3489185d19ade26914272 ed757fde810048e7b798d984e9dfeb49 - - -] 192.168.0.20 - - " +
      "[10/Feb/2017 06:18:07] \"GET /v1/images/detail?is_public=None&limit=20 HTTP/1.1\" 200 2862 0.290697"
    Assert.assertTrue(ProcessorHelper.getFieldFromString(lineOfLog, "", 3) == "INFO")
  }*/

  /*@Test
  def testTimestampToTimeFrame() = {
    val lineOfLog: String = "2017-02-10 06:18:07.264 3397 INFO eventlet.wsgi.server " +
      "[req-08ef6dd2-4f3b-44ae-8d16-992adcc009ef" +
      " acab852ba0b3489185d19ade26914272 ed757fde810048e7b798d984e9dfeb49 - - -] 192.168.0.20 - - " +
      "[10/Feb/2017 06:18:07] \"GET /v1/images/detail?is_public=None&limit=20 HTTP/1.1\" 200 2862 0.290697"
    val stringtimestamp: String = new String(ProcessorHelper.getFieldFromString(lineOfLog, "", 0)
      + " " + ProcessorHelper.getFieldFromString(lineOfLog, "", 1))
    Assert.assertEquals(stringtimestamp, "2017-02-10 06:18:07.264")
    val millis: Long = Timestamp.valueOf(stringtimestamp).getTime
    val minutes: Long = TimeUnit.MILLISECONDS.toMinutes(millis)
    Assert.assertEquals(minutes, 24778398L)

  }*/

 /* @Test
  def testTimeFrameInMinutes() = {
    val lineOfLog = "2016-03-29 07:58:09.232 2535 INFO eventlet.wsgi.server [req-d34145b2-f2bb-4cdc-9399-94a2bdc4f67c" +
      " " +
      "acab852ba0b3489185d19ade26914272 ed757fde810048e7b798d984e9dfeb49 - - -] 192.168.0.20 - - [29/Mar/2016 " +
      "07:58:09] " +
      "HEAD /v1/images/fb263421-65d9-4d7d-bf87-d431eaf624d8 HTTP/1.1 200 1257 0.443069"
    val pieceTime: String = ProcessorHelper.getFieldFromString(lineOfLog, "", 1)
    Assert.assertEquals(ProcessorHelper.getMinutesFromTimePieceLogLine(pieceTime), (7 * 60 + 58))
  }*/



  @Test
  def testUserOfreadCsvFileFromBatchEnvironment() = {
    val envBatch = ExecutionEnvironment.getExecutionEnvironment
    val tablesLoaded: DataSet[String] = envBatch.readTextFile("./src/main/resources/tables/tables.csv")
    val datasetTables: DataSet[DbTable] = tablesLoaded.map(s => {
      new DbTable(s.split(";")(0), s.split(";").slice(1, s
        .size - 1): _*)
    })
    val a = datasetTables.map(Assert.assertNotNull(_))
  }

  @Test
  def testUseOfReadFileOfPrimitives() = {
    val envBatch = ExecutionEnvironment.getExecutionEnvironment
    val b: DataSet[_ <: Any] = envBatch.readFileOfPrimitives[String]("./src/main/resources/types/types")
  }
}

