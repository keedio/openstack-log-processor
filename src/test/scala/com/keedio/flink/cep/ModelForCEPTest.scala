package com.keedio.flink.cep

import com.keedio.flink.OpenStackLogProcessor._
import com.keedio.flink.cep.alerts.ErrorAlert
import com.keedio.flink.cep.patterns.ErrorAlertPattern
import com.keedio.flink.entities.LogEntry
import com.keedio.flink.utils.{ProcessorHelper, SyslogCode, UtilsForTest}
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.hamcrest.MatcherAssert._
import org.hamcrest.number.OrderingComparisons._
import org.joda.time.{DateTime, Minutes}
import org.junit._

import scala.collection.immutable.Seq

/**
  * Created by luislazaro on 4/4/17.
  * lalazaro@keedio.com
  * Keedio
  */
class ModelForCEPTest {
  val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
  env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

  @After
  def executeEnvironment() = {
   Assert.assertNotNull(env.execute())
  }

  /**
    * test generation of datastream of ErrorAlert
    */
  @Test
  def toErrorAlertStreamTest() = {
    val listOfTimestamps: Seq[String] = UtilsForTest.generateTimestamps()
    val listOfDummyLogs: Seq[String] = UtilsForTest.generateListOflogs(listOfTimestamps)
    val stream: DataStream[String] = env.fromCollection(listOfDummyLogs)
    //parse json as LogEntry
    val streamOfLogs: DataStream[LogEntry] = stream.map(string => LogEntry(string))
    val streamOfLogsTimestamped: DataStream[LogEntry] = streamOfLogs.assignTimestampsAndWatermarks(new AscendingTimestampExtractor[LogEntry] {
      override def extractAscendingTimestamp(logEntry: LogEntry): Long = {
        ProcessorHelper.toTimestamp(logEntry.timestamp).getTime
      }
    }).keyBy(_.service)

    val alerts: DataStream[ErrorAlert] = toAlertStream(streamOfLogsTimestamped, new ErrorAlertPattern)

    //if alert is generated, must be belong to same service.
    alerts.map(alert => Assert.assertTrue(alert.logEntry0.service == alert.logEntry1.service))

    //is alert is generated, timestamps must belong to interval (0, 10]
    alerts.map(alert => {
      val time0 = new DateTime(ProcessorHelper.toTimestamp(alert.logEntry0.timestamp))
      val time1 = new DateTime(ProcessorHelper.toTimestamp(alert.logEntry1.timestamp))
      val diffMinutes: Int = Minutes.minutesBetween(time1, time0).getMinutes
      assertThat(Integer.valueOf(diffMinutes), lessThanOrEqualTo(Integer.valueOf(10)))
    })

    //logentries must have the same severity
    alerts.map(alert => {
      val severity0 = SyslogCode(alert.logEntry0.severity)
      val severity1 = SyslogCode(alert.logEntry1.severity)
      Assert.assertEquals(severity0, severity1)
    })

    alerts.map(alert => Assert.assertTrue(alert.isInstanceOf[ErrorAlert]))

    alerts.map(alert => {
      Assert.assertNotSame(alert.logEntry0, alert.logEntry1)
    })
    alerts.rebalance.print
    println
  }

}
