package com.tikal.spark.workshop.stream

import org.apache.spark.SparkContext._
import org.apache.spark._
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming._

/**
  * Find top words.
  *
  * @author Dmitri Krasnenko
  */
object TopWordsFileStream {
   def main(args: Array[String]) {
     val ssc = new StreamingContext(
       new SparkConf().setMaster("local").setAppName("stream-wc"),
       Seconds(15)
     )


     ssc.textFileStream(args(0)).map((_, 1)).reduceByKeyAndWindow(_+_, _-_, Seconds(30))
       .map(_.swap)
       .transform(_.sortByKey(false)).print()

     ssc.start()
     ssc.awaitTermination()
   }

 }
