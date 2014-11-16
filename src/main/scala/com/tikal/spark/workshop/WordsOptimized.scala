package com.tikal.spark.workshop

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

/**
 * Words count, more optimized solution.
 *
 * @author Dmitri Krasnenko
 */
object WordsOptimized {
  def main(args: Array[String]) {
    val sc: SparkContext = new SparkContext("local[4]", "words-optimized")

    // 1. Read lines from a text file(s)
    // 2. Create word -> count tuple
    // 3. Calculate number of unique words by reducing the stream
    // 4. Return a collection of [word -> count] tuples back to the driver
    val words = sc.textFile(args(0))
      .map(name => (name, 1))
      .reduceByKey(_ + _)
      .collect()

    //Print words out
    println(words)
  }

}
