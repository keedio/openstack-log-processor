package com.keedio.flink

import com.keedio.flink.entities.{DbTable, LogEntry}
import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment, createTypeInformation}
import org.apache.flink.streaming.api.functions.co.{CoFlatMapFunction, CoMapFunction}
import org.apache.flink.streaming.api.scala.{ConnectedStreams, DataStream, StreamExecutionEnvironment}
import org.apache.flink.util.Collector
import org.junit.{Assert, Test}

/**
  * Created by luislazaro on 14/2/17.
  * lalazaro@keedio.com
  * Keedio
  */

class OpenStackLogProcessorTest {


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

  @Test
  def testGetDbTupleFromDbTable() = {
    val envBatch = ExecutionEnvironment.getExecutionEnvironment
    val source: DataSet[String] = envBatch.readTextFile("./src/test/resources/example2.log")
    val streamOfLogs: DataSet[LogEntry] = source.map(s => LogEntry(s))

    val tablesLoaded: DataSet[String] = envBatch.readTextFile("./src/test/resources/tablesFile.txt")
    val dtTables: DataSet[DbTable] = tablesLoaded.map(s => new DbTable(s.split(":")(0), s.split(":")(1).split(";"): _*))
    val listOfDbTables: Seq[DbTable] = dtTables.collect()

  }

  @Test
  def testConnectedStreamsMap() = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val streamAnimals: DataStream[String] = env.fromCollection(Seq("bird", "dog", "spider"))
    val streamLegs: DataStream[Int] = env.fromCollection(Seq(2, 4, 8))
    val connectedStreamAnimals: ConnectedStreams[String, Int] = streamAnimals.connect(streamLegs)

    val a: DataStream[Boolean] = connectedStreamAnimals.map(new CoMapFunction[String, Int, Boolean] {
      override def map1(in1: String): Boolean = true

      override def map2(in2: Int): Boolean = false
    })

    val b: DataStream[Boolean] = connectedStreamAnimals.map(
      (_: String) => true,
      (_: Int) => false
    )

    val c: DataStream[String] = connectedStreamAnimals.map(
      (in1: String) => in1,
      (in2: Int) => in2.toString
    )

    val d = connectedStreamAnimals.map(new CoMapFunction[String, Int, String] {
      override def map1(in1: String): String = in1

      override def map2(in2: Int): String = in2.toString
    })

    a.rebalance.print
    b.rebalance.print
    c.rebalance.print
    d.rebalance.print
    env.execute()
    println("end")
  }

  @Test
  def testConnectedStreamsFlatMap() = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val streamAnimals: DataStream[String] = env.fromCollection(Seq("bird", "dog", "spider"))
    val streamLegs: DataStream[Int] = env.fromCollection(Seq(2, 4, 8))
    val connectedStreamAnimals: ConnectedStreams[String, Int] = streamAnimals.connect(streamLegs)

    val a: DataStream[Int] = connectedStreamAnimals.flatMap(new CoFlatMapFunction[String, Int, Int] {
      override def flatMap1(in1: String, collector: Collector[Int]): Unit = {
        in1.length > 0 match {
          case true => collector.collect(in1.length)
          case false => collector.collect(0)
        }
      }

      override def flatMap2(in2: Int, collector: Collector[Int]): Unit = collector.collect(in2)
    })

    a.rebalance.print

    env.execute()
    println("end")
  }

  @Test
  def testConnectStreamsExample() = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val data: DataStream[String] = env.fromElements("bird", "dog", "spider", "elephant", "to")
    val control: DataStream[Int] = env.fromElements(7)

    val filtered = data.connect(control.broadcast).flatMap(new CoFlatMapFunction[String, Int, String]  {
      var length = 0

      //filter strings by length
      override def flatMap1(in1: String, collector: Collector[String]): Unit = {
        if (in1.length > length) collector.collect(in1)
      }

      //receive new filter length
      override def flatMap2(in2: Int, collector: Collector[String]): Unit = {
        length = in2
      }

//      override def snapshotState(l: Long, l1: Long): Integer = length
//
//      override def restoreState(t: Integer): Unit = {length = t}
    })
    filtered.rebalance.print()
    env.execute()

    println
  }


}

