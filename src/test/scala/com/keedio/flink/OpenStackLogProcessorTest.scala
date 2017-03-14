package com.keedio.flink

import com.keedio.flink.entities.{DbTable, LogEntry}
import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment, createTypeInformation}
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


}

