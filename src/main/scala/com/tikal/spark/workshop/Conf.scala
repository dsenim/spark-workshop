package com.tikal.spark.workshop

import org.codehaus.jackson.map.ObjectMapper
import org.slf4j.{Logger, LoggerFactory}

/**
 * Conf 
 *
 * @author Dmitri Krasnenko
 */
object Conf {
  val SPARK_DATA = "/var/lib/spark/data"

  val NAMES_PATH = s"$SPARK_DATA/names"
  val HELLO_PATH = s"$SPARK_DATA/hello.txt"
  val NAMES_RAWS_PATH = s"$SPARK_DATA/names.txt"
  val NAMES_JSON_PATH = s"$SPARK_DATA/names.json"

  val MAPPER: ObjectMapper = new ObjectMapper()

  val LOGGER:Logger = LoggerFactory.getLogger("com.tikal.spark.workshop")
}
