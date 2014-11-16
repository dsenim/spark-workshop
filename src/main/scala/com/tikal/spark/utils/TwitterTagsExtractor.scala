package com.tikal.spark.utils

import com.tikal.spark.workshop.Conf
import org.apache.spark.SparkContext

import scala.collection.JavaConversions._

/**
 * Twitter tags extractor
 *
 * @author Dmitri Krasnenko
 */
object TwitterTagsExtractor {
  def main(args: Array[String]) {
    new SparkContext("local[4]", "tags-extractor")
      .textFile(args(0))
      .map(Conf.MAPPER.readValue(_, classOf[java.util.HashMap[String, Object]]))
      .flatMap(_.get("entities").asInstanceOf[java.util.Map[String, Object]].toMap).filter(_._1.equals("hashtags")).flatMap(_._2.asInstanceOf[java.util.List[java.util.Map[String,String]]].toList).flatMap(_.toMap.get("text"))
      .saveAsTextFile(args(1))
  }

}
