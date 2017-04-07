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

  def mapOfAsserts(alertsStream: DataStream[ErrorAlert]): Unit = {
    alertsStream.map(alert => {
      //if alert is generated, must be belong to the same service.
      Assert.assertTrue(alert.logEntry0.service == alert.logEntry1.service)

      //if alert is generated, timestamps must belong to interval (0, 10]
      val time0 = new DateTime(ProcessorHelper.toTimestamp(alert.logEntry0.timestamp))
      val time1 = new DateTime(ProcessorHelper.toTimestamp(alert.logEntry1.timestamp))
      val diffMinutes: Int = Minutes.minutesBetween(time1, time0).getMinutes
      assertThat(Integer.valueOf(diffMinutes), lessThanOrEqualTo(Integer.valueOf(10)))

      //logentries must have the same severity
      Assert.assertEquals(SyslogCode(alert.logEntry0.severity), SyslogCode(alert.logEntry1.severity))
      Assert.assertTrue(alert.isInstanceOf[ErrorAlert])
      Assert.assertNotSame(alert.logEntry0, alert.logEntry1)
      Assert.assertNotEquals(alert.logEntry0, alert.logEntry1)
      Assert.assertTrue(alert.logEntry0.id != alert.logEntry1.id)
    })
  }

  /**
    * test generation of datastream of ErrorAlert
    */
  @Test
  def toErrorAlertStreamTest() = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val listOfTimestamps: Seq[String] = UtilsForTest.generateTimestamps()
    val listOfDummyLogs: Seq[String] = UtilsForTest.generateListOflogs(listOfTimestamps)
    val stream: DataStream[String] = env.fromCollection(listOfDummyLogs)
    //parse json as LogEntry
    val streamOfLogs: DataStream[LogEntry] = stream.map(string => LogEntry(string))
    val streamOfLogsTimestamped: DataStream[LogEntry] = streamOfLogs.assignTimestampsAndWatermarks(
      new AscendingTimestampExtractor[LogEntry] {
        override def extractAscendingTimestamp(logEntry: LogEntry): Long = {
          ProcessorHelper.toTimestamp(logEntry.timestamp).getTime
        }
      }).keyBy(_.service)

    val alerts: DataStream[ErrorAlert] = toAlertStream(streamOfLogsTimestamped, new ErrorAlertPattern)
    alerts.rebalance.print

    mapOfAsserts(alerts)
    Assert.assertNotNull(env.execute())
  }

  @Test
  def toErroralertSimpleStreamTestMatchedPattern() = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val json0 = "{\"severity\":\"7\",\"body\":\"nova-compute - - - 2017-04-06 14:14:47.422 595544 ERROR " +
      "oslo_messaging._drivers.amqpdriver [req-c4434725-9b8d-4fe3-b41c-fcf7b89869a5 - - - - -]\"," +
      "\"spriority\":\"191\"," +
      "\"hostname\":\"overcloud-compute-1\",\"protocol\":\"TCP\",\"port\":\"7790\",\"sender\":\"/192.168.1.16\"," +
      "\"service\":\"Nova\",\"id\":\"1491481427635_2017-04-06 14:14:47.422_545262019\",\"facility\":\"23\"," +
      "\"timestamp\":\"2017-04-06T12:53:32.420009+00:00\"}"

    val json1 = "{\"severity\":\"7\",\"body\":\"nova-compute - - - 2017-04-06 14:10:47.422 595544 ERROR " +
      "oslo_messaging._drivers.amqpdriver [req-c4434725-9b8d-4fe3-b41c-fcf7b89869a5 - - - - -]\"," +
      "\"spriority\":\"191\"," +
      "\"hostname\":\"overcloud-compute-1\",\"protocol\":\"TCP\",\"port\":\"7790\",\"sender\":\"/192.168.1.16\"," +
      "\"service\":\"Nova\",\"id\":\"1844a3ce-c2ae-4417-8c6b-0243491bfec2\",\"facility\":\"23\"," +
      "\"timestamp\":\"2017-04-06T12:53:32.420009+00:00\"}"

    val listOfDummyLogs: Seq[String] = Seq(json0, json1)
    val stream: DataStream[String] = env.fromCollection(listOfDummyLogs)
    val streamOfLogs: DataStream[LogEntry] = stream.map(string => LogEntry(string))
    streamOfLogs.rebalance.print
    val streamOfLogsTimestamped: DataStream[LogEntry] = streamOfLogs.assignTimestampsAndWatermarks(
      new AscendingTimestampExtractor[LogEntry] {
        override def extractAscendingTimestamp(logEntry: LogEntry): Long = {
          ProcessorHelper.toTimestamp(logEntry.timestamp).getTime
        }
      }).keyBy(_.service)

    val alerts: DataStream[ErrorAlert] = toAlertStream(streamOfLogsTimestamped, new ErrorAlertPattern)

    alerts.rebalance.print

    alerts.map(alert => println("==============================>>> " + alert.logEntry0))

    mapOfAsserts(alerts)

    Assert.assertNotNull(env.execute())
  }


  @Test
  def toErroralertSimpleStreamTestNOTMatchedPattern() = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val json0 = "{\"severity\":\"7\",\"body\":\"nova-compute - - - 2017-04-07 09:27:59.746 595544 ERROR " +
      "oslo_messaging._drivers.amqpdriver [req-c4434725-9b8d-4fe3-b41c-fcf7b89869a5 - - - - -]\"," +
      "\"spriority\":\"191\"," +
      "\"hostname\":\"overcloud-compute-1\",\"protocol\":\"TCP\",\"port\":\"7790\",\"sender\":\"/192.168.1.16\"," +
      "\"service\":\"Nova\",\"id\":\"1491550440023_2017-04-07 08:42:59.746_1025179506\",\"facility\":\"23\"," +
      "\"timestamp\":\"2017-04-06T12:53:32.420009+00:00\"}"

    val json1 = "{\"severity\":\"7\",\"body\":\"nova-compute - - - 2017-04-07 08:34:59.746 595544 ERROR " +
      "oslo_messaging._drivers.amqpdriver [req-c4434725-9b8d-4fe3-b41c-fcf7b89869a5 - - - - -]\"," +
      "\"spriority\":\"191\"," +
      "\"hostname\":\"overcloud-compute-1\",\"protocol\":\"TCP\",\"port\":\"7790\",\"sender\":\"/192.168.1.16\"," +
      "\"service\":\"Nova\",\"id\":\"1491550440022_2017-04-07 08:49:59.746_339949963\",\"facility\":\"23\"," +
      "\"timestamp\":\"2017-04-06T12:53:32.420009+00:00\"}"

    val listOfDummyLogs: Seq[String] = Seq(json0, json1)
    val stream: DataStream[String] = env.fromCollection(listOfDummyLogs)
    val streamOfLogs: DataStream[LogEntry] = stream.map(string => LogEntry(string))
    streamOfLogs.rebalance.print
    val streamOfLogsTimestamped: DataStream[LogEntry] = streamOfLogs.assignTimestampsAndWatermarks(
      new AscendingTimestampExtractor[LogEntry] {
        override def extractAscendingTimestamp(logEntry: LogEntry): Long = {
          ProcessorHelper.toTimestamp(logEntry.timestamp).getTime
        }
      }).keyBy(_.service)

    val alerts: DataStream[ErrorAlert] = toAlertStream(streamOfLogsTimestamped, new ErrorAlertPattern)

    alerts.rebalance.print

    alerts.map(alert => print("==============================>>> " + alert.logEntry0))

    mapOfAsserts(alerts)

    Assert.assertNotNull(env.execute())
  }
}
