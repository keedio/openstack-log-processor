package com.keedio.flink.mappers

import com.keedio.flink.cep.alerts.ErrorAlert
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.configuration.Configuration
import org.apache.flink.metrics.Counter
import org.apache.flink.api.java.tuple.Tuple5
/**
  * Created by luislazaro on 11/4/17.
  * lalazaro@keedio.com
  * Keedio
  */
class RichMapFunctionAlertServices extends RichMapFunction[ErrorAlert, Tuple5[String, String, String, String, String]] {
  var counter: Counter = _

  @throws(classOf[Exception])
  override def map(errorAlert: ErrorAlert): Tuple5[String, String, String, String, String] = {
    this.counter.inc()
    new Tuple5(errorAlert.getAlertName, errorAlert.logEntry0.service, errorAlert.logEntry0.timestamp,
      errorAlert.logEntry1.timestamp, errorAlert.toString)
  }

  override def open(configuration: Configuration) = {
    this.counter = getRuntimeContext
      .getMetricGroup
      .counter("Total_Alerts_To_Alerts_Services")
  }
}
