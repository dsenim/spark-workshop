package com.tikal.spark.workshop.stream

import org.apache.spark._
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming._

/**
 * Words count stream sample.
 *
 * @author Dmitri Krasnenko
 */
object WordsStreamingFIleStream {
  def main(args: Array[String]) {
    val ssc = new StreamingContext(
      new SparkConf().setMaster("local[6]").setAppName("stream-wc"),
      Seconds(15)
    )

    ssc.checkpoint("/var/lib/spark/data/checkpoint")

    val f = ssc.textFileStream("/var/lib/spark/data/tags")
      .map((_,1))
      .reduceByKey(_ + _).cache()

    f.foreachRDD{
      _.foreach(println(_))
    }

    ssc.start()
    ssc.awaitTermination()
  }

}
