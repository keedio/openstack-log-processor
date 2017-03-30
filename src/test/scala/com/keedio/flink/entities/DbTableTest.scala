package com.keedio.flink.entities

import org.apache.flink.api.java.tuple.{Tuple, Tuple3, Tuple4}
import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment, createTypeInformation}
import org.junit.{Assert, Test}

import scala.io.Source

/**
  * Created by luislazaro on 13/3/17.
  * lalazaro@keedio.com
  * Keedio
  */
class DbTableTest {
  val pathToTableFile = "./src/test/resources/tablesFile.txt"

  @Test
  def dbTableContainsAllRequiredFieldsTest() = {
    val dbTable = new DbTable("tabla1", Seq("columa1", "columna2", "columna3"): _*)
    Assert.assertEquals(dbTable.getClass.getDeclaredFields.length, 4)
  }

  @Test
  def fieldTupleContainsSameNumberOfElementsThanColumnsTest = {
    val dbTable = new DbTable("tabla1", Seq("columa1", "columna2", "columna3"): _*)
    Assert.assertEquals(dbTable.getClass.getDeclaredFields.length, 4)
    Assert.assertEquals(dbTable.tuple.getArity, 3)
    Assert.assertTrue(dbTable.tuple.isInstanceOf[Tuple3[_, _, _]])
    Assert.assertFalse(dbTable.tuple.isInstanceOf[Tuple4[_, _, _, _]])
  }

  @Test
  def loadTablesFromExternalFileTest() = {
    require(pathToTableFile != null)
    val loadedDbTable: Iterator[String] = Source.fromFile(pathToTableFile).getLines()
    val listOfTables: Iterator[DbTable] = loadedDbTable.map(line => {
      val tableName: String = line.split(":")(0).trim
      val columns: Array[String] = line.split(":")(1).trim.split(";")
      new DbTable(tableName, columns: _*)
    })
    listOfTables.foreach(dbTable => Assert.assertEquals(dbTable.tuple.getArity, dbTable.cols.length))
  }

  @Test
  def loadTablesFromExternalFileTestViaExecutionEnvironment() = {
    require(pathToTableFile != null)
    val envBatch = ExecutionEnvironment.getExecutionEnvironment
    val tablesLoaded: DataSet[String] = envBatch.readTextFile(pathToTableFile)
    val dtTables: DataSet[DbTable] = tablesLoaded.map(s => new DbTable(s.split(":")(0).trim, s.split(":")(1).trim.split(";"): _*))
    val listOfDbTables: Seq[DbTable] = dtTables.collect()
    listOfDbTables.foreach(dbTable => Assert.assertEquals(dbTable.tuple.getArity, dbTable.cols.length))
  }

  @Test
  def setFieldsOfArgumentTupleTest() = {
    val dbTable = new DbTable("tabla1", Seq("columna1", "columna2", "columna3"): _*)
    val xTuple: Tuple = dbTable.tuple
    //set value for all fields of Xtuple
    dbTable.cols.foreach(col => xTuple.setField(col, dbTable.cols.indexOf(col)))
    for (i <- 0 to xTuple.getArity - 1) {
      Assert.assertNotNull(xTuple.getField(i))
    }

    Assert.assertTrue(xTuple.isInstanceOf[Tuple])
    Assert.assertTrue(xTuple.isInstanceOf[Tuple3[_, _, _]])
    Assert.assertFalse(xTuple.isInstanceOf[Tuple4[_, _, _,_]])
  }


}
