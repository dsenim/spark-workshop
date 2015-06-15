package com.tikal.spark.workshop

import com.datastax.spark.connector._
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Finding top-10 words using Cassandra
 *
 * For more info of how to use C* connector, follow the link https://github.com/datastax/spark-cassandra-connector
 *
 * @author Dmitri Krasnenko
 */
object WordsPersistentCassandra {
  def main(args: Array[String]) {
    val conf = new SparkConf(true)
      .set("spark.cassandra.connection.host", "127.0.0.1")

    val sc = new SparkContext("local[4]", "words-cassandra", conf)

    //Create [tag -> cont] unique tuples and save them to C*
    sc.textFile(Conf.NAMES_PATH).map(name => (name, 1))
      .reduceByKey(_ + _)
      .saveToCassandra("spark_workshop", "tags", SomeColumns("tag", "count"))

    //Read counters from C* and find the 5 most popular tags
    val top = sc.cassandraTable[(Int, String)]("spark_workshop", "tags").select("count", "tag")
      .sortBy(_._1, false)
      .take(5)

    top.foreach(println(_))
  }

}
