package com.keedio.flink.cep

import org.apache.flink.cep.scala.pattern.Pattern

/**
  * Created by luislazaro on 31/3/17.
  * lalazaro@keedio.com
  * Keedio
  */

/**
  * An Alert Pattern describes the pattern of an Alert, which is triggered by an Event
  * @tparam TEventType
  * @tparam TAlertType
  */
trait IAlertPattern[TEventType, TAlertType <: IAlert] extends Serializable{

  /**
    * Implements the mapping between the pattern matching result and the alert.
    * @param pattern Pattern, with has been match by Apache flink
    * @return the alert created from the given match result
    */
  def create(pattern: java.util.Map[String, TEventType]): TAlertType

  /**
    * Implements the apache flink Cep Event Pattern which triggers an alert.
    * @return
    */
  def getEventPattern(): Pattern[TEventType, _]

}
