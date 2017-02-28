package com.keedio.flink.integration

import com.keedio.flink.entities.LogEntry
import org.apache.flink.api.java.tuple._
import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment, createTypeInformation}
import org.junit.Test

/**
  * Created by luislazaro on 23/2/17.
  * lalazaro@keedio.com
  * Keedio
  */
class LogEntrySimpleProcessorTest {


  def logEntryToTuple(streamOfLogs: DataSet[LogEntry]): DataSet[_<: Tuple] = {
    streamOfLogs.map(logEntry => {
      new Tuple5[String, String, String, String, String](
        logEntry.valuesMap("date"),
        logEntry.valuesMap("time"),
        logEntry.valuesMap("pid"),
        logEntry.valuesMap("loglevel"),
        logEntry.valuesMap("remainingLog")
      )
    })

  }



  @Test
  def logEntryToTupleManuallyTest() = {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val stream: DataSet[String] = env.readTextFile("./src/test/resources/example.log")
    //stream.rebalance.print
    val streamOfLogs: DataSet[LogEntry] = stream.rebalance.map(string => new LogEntry(string, Seq("date", "time", "pid", "loglevel"), " "))
    val streamOfTuples: DataSet[Tuple5[String, String, String, String, String]] = streamOfLogs.map(logEntry => {
      new Tuple5[String, String, String, String, String](
        logEntry.valuesMap("date"),
        logEntry.valuesMap("time"),
        logEntry.valuesMap("pid"),
        logEntry.valuesMap("loglevel"),
        logEntry.valuesMap("remainingLog")
      )
    })

   streamOfTuples.rebalance.print
   print("end")
  }


  @Test
  def logEntryToTupleTest() = {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val stream: DataSet[String] = env.readTextFile("./src/test/resources/example.log")
    val streamOfLogs: DataSet[LogEntry] = stream.rebalance.map(string => new LogEntry(string, Seq("date", "time", "pid", "loglevel"), " "))

    logEntryToTuple(streamOfLogs).rebalance().print
    print("end2")
  }


}
