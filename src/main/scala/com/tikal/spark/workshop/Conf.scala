package com.tikal.spark.workshop

import org.codehaus.jackson.map.ObjectMapper
import org.slf4j.{Logger, LoggerFactory}

/**
 * Conf 
 *
 * @author Dmitri Krasnenko
 */
object Conf {
  val MAPPER: ObjectMapper = new ObjectMapper()

  val LOGGER:Logger = LoggerFactory.getLogger("com.tikal.spark.workshop")
}
