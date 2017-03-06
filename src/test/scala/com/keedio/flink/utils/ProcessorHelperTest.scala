package com.keedio.flink.utils

import java.sql.Timestamp
import java.util.Date

import org.apache.commons.lang3.time.DateUtils
import org.joda.time.DateTime
import org.junit.Test

/**
  * Created by luislazaro on 27/2/17.
  * lalazaro@keedio.com
  * Keedio
  */
class ProcessorHelperTest {

  @Test
  def getTimesTampFromSyslog: Unit = {
    Timestamp.valueOf("2016-02-03 16:05:00")
    val ta: Timestamp = ProcessorHelper.toTimestamp("Mar 4 2017 18:15:06")
    val dta = new DateTime(ta)
    println("----" + dta)
    print(ProcessorHelper.toTimestamp("Mar 4 2017 18:15:06"))
    val undtatime: DateTime = ProcessorHelper.getParsedTimestamp("Feb  28 2017 12:23:50")
    println(undtatime)
    val listOfFormats: Seq[String] = Seq("MMM dd HH:mm:ss")
   val adate: Date =  DateUtils.parseDateStrictly("Feb 28 12:23:50", listOfFormats: _*)
    print(new Timestamp(adate.getTime))

    println("-----> " + ProcessorHelper.getParsedTimestamp("Feb  10 2017 19:33:33"))
    println("-----> " + ProcessorHelper.toTimestamp("Feb  10 2017 19:33:33"))
    println("-----> " + new DateTime(ProcessorHelper.toTimestamp("Feb  10 2017 19:33:33")))

  }

//  @Test(Ignore)
//  def testGenerate() = {
//    print(ProcessorHelper.generateListOflogs(ProcessorHelper.generateTimestamps()))
//  }

}
