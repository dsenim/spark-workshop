package com.tikal.spark.workshop

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

/**
 * Words count naive implementation. The code is highly inefficient and written with one only purpose:
 * to demonstrate how ugly ideas may affect the computation.
 *
 * NOTE: Not a production ready code.
 *
 * @author Dmitri Krasnenko
 */
object WordsNaive {
  def main(args: Array[String]) {
    val sc: SparkContext = new SparkContext("local[4]", "words-naive")

    // 1. Read lines from a text file(s)
    // 2. Create a [word -> word] tuple
    // 3. Create multi value map by grouping tuples with the same left element
    // 4. Calculate number of words in each collection
    // 5. Return a collection of [word -> count] tuples back to the driver
    val words = sc.textFile(Conf.NAMES_PATH).map(name => (name, name)).groupByKey().mapValues(names => names.toArray.length).collect()

    //Print words out
    words.foreach(pair => println(s"[${pair._1}:${pair._2}]"))
  }

}
