package com.keedio.flink.utils

/**
  * Created by luislazaro on 27/2/17.
  * lalazaro@keedio.com
  * Keedio
  */

/**
  * Functions in ProcessorHelperPoc are utilities for generation random and/or dummy data for POC.
  */
object ProcessorHelperPoc {

  /**
    * Only for poc: insert randomn node_types
    */
  def generateRandomNodeType: String = {
    val servicesMap = Map(
      0 -> "compute",
      1 -> "storage"
    )
    val rand = scala.util.Random
    val randKey = rand.nextInt(1)
    servicesMap(randKey)
  }

  /**
    * Only for poc: insert services
    */
  def generateRandomService: String = {
    val servicesMap = Map(
      0 -> "Nova",
      1 -> "Keystone",
      2 -> "Pacemaker",
      3 -> "Neutron",
      4 -> "Storage",
      5 -> "Cinder",
      6 -> "Glance",
      7 -> "Swift"
    )
    val rand = scala.util.Random
    val randKey = rand.nextInt(7)
    servicesMap(randKey)
  }
}
