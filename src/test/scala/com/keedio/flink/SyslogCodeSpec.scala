package com.keedio.flink

import com.keedio.flink.utils.SyslogCode

/**
  * Created by luislazaro on 22/3/17.
  * lalazaro@keedio.com
  * Keedio
  */
class SyslogCodeSpec extends UnitSpec {

  "severity" must "contains the provided log-level" in {
    val logLevel = "INFO"
    assert(SyslogCode.severity.values.toSeq.contains(logLevel))
    SyslogCode.severity.values.toSeq should contain(logLevel)
  }

  it must "not contains any other log-level" in {
    SyslogCode.severity.values.toSeq should not contain ("OTHER")
  }

  it should "throw IllegalArgumentException if an invalid key is provided" in {
    val listOfKeys: Seq[Int] = SyslogCode.severity.keys.map(_.toInt).toSeq.sorted

    assertThrows[IllegalArgumentException] {
      SyslogCode((listOfKeys.head - 1).toString)
    }

    assertThrows[IllegalArgumentException] {
      SyslogCode((listOfKeys.last + 1).toString)
    }
  }
}
