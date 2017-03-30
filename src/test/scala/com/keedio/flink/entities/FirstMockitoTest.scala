package com.keedio.flink.entities

import org.junit.{Assert, Test}
import org.mockito.Mockito
import org.mockito.Mockito._

/**
  * Created by luislazaro on 22/3/17.
  * lalazaro@keedio.com
  * Keedio
  */
class FirstMockitoTest {
 val myLogEntry = Mockito.mock(classOf[LogEntry])

  @Test
  def testIfmyLogEntryisALogEntry() = {
    Assert.assertTrue(myLogEntry.isInstanceOf[LogEntry])
  }

  @Test
  def testDefaultBehaviourOfTestDouble() = {
    Assert.assertFalse("new test double should return false as boolean", myLogEntry.isValid())
  }

  @Test
  def testStubbing() = {
    Assert.assertFalse("new test double should return false as boolean", myLogEntry.isValid())
    when(myLogEntry.isValid()).thenReturn(true)
    Assert.assertTrue("after instructed test double should returng what we want", myLogEntry.isValid())
  }

  @Test(expected = classOf[Exception])
  def throwException() = {
    when(myLogEntry.isValid()).thenThrow(new Exception)
    myLogEntry.isValid()
    print()
  }

  @Test
  def testVerification() = {
    myLogEntry.isValid()
    verify(myLogEntry).isValid()
    println
  }


}
