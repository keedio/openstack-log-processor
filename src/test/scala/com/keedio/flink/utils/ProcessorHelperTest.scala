package com.keedio.flink.utils

import org.joda.time._
import org.junit.{Assert, Test}

/**
  * Created by luislazaro on 27/2/17.
  * lalazaro@keedio.com
  * Keedio
  */
class ProcessorHelperTest {
  /**
    * Se trata de validar la hora impresa en log frente a un rango de horas al que debe pertencer
    * el timeframe es la hora del log multiplicada por 60 minutos.
    */
  @Test
  def testForValidityTimeFrame() = {
    val valKeys = Seq(3600, 21600, 43200, 86400, 604800, 2419200)

    val eightOclockMorning: DateTime = new DateTime(2017, 2, 22, 8, 0)
    val timeFrameUpper_eight = 21 * 60 + 20
    val timeFrameLower_eight = 3 * 60 + 59
    val timeFrameMiddle_eight = 8 * 60 + 0

    //now is 08:00 and log is 21:00 belongs to {12h, 24h, 1m, 1w}
    valKeys.foreach(valkey => {
      valkey match {
        case 3600 => Assert.assertFalse(ProcessorHelper.isValidTimeFrame(timeFrameUpper_eight, 3600,
          eightOclockMorning))
        case 21600 => Assert.assertFalse(ProcessorHelper.isValidTimeFrame(timeFrameUpper_eight, 21600,
          eightOclockMorning))
        case 43200 => Assert.assertTrue(ProcessorHelper.isValidTimeFrame(timeFrameUpper_eight, 43200,
          eightOclockMorning))
        case 86400 => Assert.assertTrue(ProcessorHelper.isValidTimeFrame(timeFrameUpper_eight, 86400,
          eightOclockMorning))
        case 604800 => Assert.assertTrue(ProcessorHelper.isValidTimeFrame(timeFrameUpper_eight, 604800,
          eightOclockMorning))
        case 2419200 => Assert.assertTrue(ProcessorHelper.isValidTimeFrame(timeFrameUpper_eight, 2412900,
          eightOclockMorning))
      }
    })

    //now is 08:00 and log is 03:00 belongs to {6h, 12h, 24h, 1w, 1m}
    valKeys.foreach(valkey => {
      valkey match {
        case 3600 => Assert.assertFalse(ProcessorHelper.isValidTimeFrame(timeFrameLower_eight, 3600,
          eightOclockMorning))
        case 21600 => Assert.assertTrue(ProcessorHelper.isValidTimeFrame(timeFrameLower_eight, 21600,
          eightOclockMorning))
        case 43200 => Assert.assertTrue(ProcessorHelper.isValidTimeFrame(timeFrameLower_eight, 43200,
          eightOclockMorning))
        case 86400 => Assert.assertTrue(ProcessorHelper.isValidTimeFrame(timeFrameLower_eight, 86400,
          eightOclockMorning))
        case 604800 => Assert.assertTrue(ProcessorHelper.isValidTimeFrame(timeFrameLower_eight, 604800,
          eightOclockMorning))
        case 2419200 => Assert.assertTrue(ProcessorHelper.isValidTimeFrame(timeFrameLower_eight, 2412900,
          eightOclockMorning))
      }
    })

    //now is 08:00 and log is 08:00 belongs to all ranges
    valKeys.foreach(valkey => {
      valkey match {
        case 3600 => Assert.assertTrue(ProcessorHelper.isValidTimeFrame(timeFrameMiddle_eight, 3600,
          eightOclockMorning))
        case 21600 => Assert.assertTrue(ProcessorHelper.isValidTimeFrame(timeFrameMiddle_eight, 21600,
          eightOclockMorning))
        case 43200 => Assert.assertTrue(ProcessorHelper.isValidTimeFrame(timeFrameMiddle_eight, 43200,
          eightOclockMorning))
        case 86400 => Assert.assertTrue(ProcessorHelper.isValidTimeFrame(timeFrameMiddle_eight, 86400,
          eightOclockMorning))
        case 604800 => Assert.assertTrue(ProcessorHelper.isValidTimeFrame(timeFrameMiddle_eight, 604800,
          eightOclockMorning))
        case 2419200 => Assert.assertTrue(ProcessorHelper.isValidTimeFrame(timeFrameMiddle_eight, 2412900,
          eightOclockMorning))
      }
    })


    val sevenOclockEvening: DateTime = new DateTime(2017, 2, 22, 19, 0)
    val timeFrameUpper_seven = 21 * 60 + 59
    val timeFrameLower_seven = 3 * 60 + 1
    val timeFrameMiddle_seven = 19 * 60 + 0

    //now is 19:00 and log is 21:00 belongs to {24h, 1w, 1m}
    valKeys.foreach(valkey => {
      valkey match {
        case 3600 => Assert.assertFalse(ProcessorHelper.isValidTimeFrame(timeFrameUpper_seven, 3600,
          sevenOclockEvening))
        case 21600 => Assert.assertFalse(ProcessorHelper.isValidTimeFrame(timeFrameUpper_seven, 21600,
          sevenOclockEvening))
        case 43200 => Assert.assertFalse(ProcessorHelper.isValidTimeFrame(timeFrameUpper_seven, 43200,
          sevenOclockEvening))
        case 86400 => Assert.assertTrue(ProcessorHelper.isValidTimeFrame(timeFrameUpper_seven, 86400,
          sevenOclockEvening))
        case 604800 => Assert.assertTrue(ProcessorHelper.isValidTimeFrame(timeFrameUpper_seven, 604800,
          sevenOclockEvening))
        case 2419200 => Assert.assertTrue(ProcessorHelper.isValidTimeFrame(timeFrameUpper_seven, 2412900,
          sevenOclockEvening))
      }
    })

    //now is 19:00 and log is 03:00 belongs to {24h, 1w, 1m}
    valKeys.foreach(valkey => {
      valkey match {
        case 3600 => Assert.assertFalse(ProcessorHelper.isValidTimeFrame(timeFrameLower_seven, 3600,
          sevenOclockEvening))
        case 21600 => Assert.assertFalse(ProcessorHelper.isValidTimeFrame(timeFrameLower_seven, 21600,
          sevenOclockEvening))
        case 43200 => Assert.assertFalse(ProcessorHelper.isValidTimeFrame(timeFrameLower_seven, 43200,
          sevenOclockEvening))
        case 86400 => Assert.assertTrue(ProcessorHelper.isValidTimeFrame(timeFrameLower_seven, 86400,
          sevenOclockEvening))
        case 604800 => Assert.assertTrue(ProcessorHelper.isValidTimeFrame(timeFrameLower_seven, 604800,
          sevenOclockEvening))
        case 2419200 => Assert.assertTrue(ProcessorHelper.isValidTimeFrame(timeFrameLower_seven, 2412900,
          sevenOclockEvening))
      }
    })

    //now is 19:00 and log is 19:00 belongs to  all ranges
    valKeys.foreach(valkey => {
      valkey match {
        case 3600 => Assert.assertTrue(ProcessorHelper.isValidTimeFrame(timeFrameMiddle_seven, 3600,
          sevenOclockEvening))
        case 21600 => Assert.assertTrue(ProcessorHelper.isValidTimeFrame(timeFrameMiddle_seven, 21600,
          sevenOclockEvening))
        case 43200 => Assert.assertTrue(ProcessorHelper.isValidTimeFrame(timeFrameMiddle_seven, 43200,
          sevenOclockEvening))
        case 86400 => Assert.assertTrue(ProcessorHelper.isValidTimeFrame(timeFrameMiddle_seven, 86400,
          sevenOclockEvening))
        case 604800 => Assert.assertTrue(ProcessorHelper.isValidTimeFrame(timeFrameMiddle_seven, 604800,
          sevenOclockEvening))
        case 2419200 => Assert.assertTrue(ProcessorHelper.isValidTimeFrame(timeFrameMiddle_seven, 2412900,
          sevenOclockEvening))
      }
    })
  }

  @Test
  def testBuildDatetime() = {
    val lineOfLog = "2017-02-27 06:30:14.365 4025 WARNING keystone.common.wsgi [-] Could not find token: " +
      "142a0f7a3b154a16be18f9c97aa3f426"
   val dateTimeFromLog = ProcessorHelper.buildDateTimeFromFieldsLog(lineOfLog)
    val now = DateTime.now
    val seconds: Seconds = Seconds.secondsBetween(now, dateTimeFromLog)



  }
}
