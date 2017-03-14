package com.keedio.flink.entities

import org.apache.flink.api.java.tuple.Tuple

/**
  * Created by luislazaro on 20/2/17.
  * lalazaro@keedio.com
  * Keedio
  */

/**
  * DbTable: entity to keep information about table to inyect data.
  * Name of table,
  * Columns's name
  * Tuple: filed to keep track of flini.Tuple to build according number
  * of columns.
  * For Example: if table1 is (col1, col2, col3) instances of this object
  * will contain Tuple3[_, _, _]
  * @param tableName
  * @param cols
  */
class DbTable(val tableName: String, val cols: String* ) extends Serializable {

  private val typeOfTuple: Class[_ <: Tuple] = Tuple.getTupleClass(cols.size)
  val tuple: Tuple = typeOfTuple.newInstance()
}
