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
      new SparkConf().setMaster("local").setAppName("stream-wc"),
      Seconds(15)
    )

    ssc.textFileStream(args(0)).map((_,1)).reduceByKey(_ + _).print()

    ssc.start()
    ssc.awaitTermination()
  }

}
