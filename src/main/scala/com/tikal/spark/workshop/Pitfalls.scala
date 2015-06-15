package com.tikal.spark.workshop

import org.apache.spark.SparkContext

/**
 * Accumulator 
 *
 * @author Dmitri Krasnenko
 */
object Pitfalls {
  def main(args: Array[String]) {
    //Create simple spark context
    //In the real world where more advanced initialization is required, consider to work with [[org.apache.spark.SparkConf]] object
    val sc = new SparkContext("local[4]", "pitfalls")

    /**
     * Laziness
     */

    //Spark is lazy, no action - no results!
    sc.parallelize(List(1,2,3,4,5)).map(_+1)

    /**
     * Spark actions
     */

    //Don't print in action
    sc.parallelize(List(1,2,3,4,5)).map(_+1).foreach(println(_))
    //Use collect and then print
    sc.parallelize(List(1,2,3,4,5)).map(_+1).collect().foreach(print(_))

    /**
     * Global counters
     */

    //Don't use external val's for global counters, never ever
    val su = 0
    sc.parallelize(List(1,2,3,4,5)).foreach(su + _)
    println("Global sum is: " + su)

    //But use accumulators instead
    val ac = sc.accumulator(0, "Sum")
    sc.parallelize(List(1,2,3,4,5)).foreach(ac += _)
    println("Accumulator returns " + ac)

  }

}
