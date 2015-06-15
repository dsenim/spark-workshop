package com.tikal.spark.workshop.stream

import com.tikal.spark.workshop.Conf
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
       new SparkConf().setMaster("local[6]").setAppName("stream-wc"),
       Seconds(15)
     )

     ssc.checkpoint(
       s"${Conf.SPARK_DATA}/checkpoint"
     )

     ssc.textFileStream(s"${Conf.SPARK_DATA}/tags")
       .map((_, 1)).reduceByKeyAndWindow(_+_, _-_, Seconds(30))
       .map(_.swap).transform(_.sortByKey(false)).foreachRDD(rdd => rdd.take(5).foreach(print(_)))

     ssc.start()
     ssc.awaitTermination()
   }

 }
