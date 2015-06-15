package com.tikal.spark.workshop

import org.apache.spark.SparkContext

/**
 * Hello world example.
 *
 * @see hello.txt
 *
 * @author Dmitri Krasnenko
 */
object HelloWorld {
  def main(args: Array[String]) {
    //Create simple spark context
    //In the real world where more advanced initialization is required, consider to work with [[org.apache.spark.SparkConf]] object
    val sc = new SparkContext("local[4]", "hello-world")

    //Create RDD from the greeting file.
    val lines = sc.textFile(Conf.HELLO_PATH)

    //TRANSFORMATION: Add 'Hello' word at the start and '!' at the end of each line.
    val greetings = lines.map(line => s"Hello $line!")

    //ACTION: Print the lines one by one
    greetings.foreach({println(_)})
  }
}
