package com.keedio.flink.unit

import com.keedio.flink.OpenStackLogProcessor
import org.junit.{Assert, Test}

/**
  * Created by luislazaro on 14/2/17.
  * lalazaro@keedio.com
  * Keedio
  */
class OpenStackLogProcessorTest {

  @Test
  def testgetLogLevelFromString() = {
    val lineOfLog: String = "2017-02-10 06:18:07.264 3397 INFO eventlet.wsgi.server [req-08ef6dd2-4f3b-44ae-8d16-992adcc009ef" +
      " acab852ba0b3489185d19ade26914272 ed757fde810048e7b798d984e9dfeb49 - - -] 192.168.0.20 - - " +
      "[10/Feb/2017 06:18:07] \"GET /v1/images/detail?is_public=None&limit=20 HTTP/1.1\" 200 2862 0.290697"
    Assert.assertTrue(OpenStackLogProcessor.getLogLevelFromString(lineOfLog) == "INFO")

    val lineOfLog1 = "Feb 14 11:49:31 pocosop root: 2016-04-20 10:32:12.500 1144 ERROR oslo_messaging._drivers.impl_rabbit [-]" +
      " AMQP server on 192.168.0.20:5672 is unreachable: [Errno 111] ECONNREFUSED. Trying again in 2 seconds."
    Assert.assertTrue(OpenStackLogProcessor.getLogLevelFromString(lineOfLog1, "root:") == "ERROR")

  }

}
