package com.tikal.spark.workshop

import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkContext._
import org.apache.spark.{SparkConf, SparkContext}
import org.bson.{BSONObject, BasicBSONObject}

/**
 * Finding top-5 words by using MongoDB as a persistent store for saving intermediate results.
 *
 * @author Dmitri Krasnenko
 */
object WordsPersistentMongodb {
  def main(args: Array[String]) {
    val sc = new SparkContext("local[4]", "tags-mongo", new SparkConf(true))

    //For more options see MongoConfigUtil
    val conf = new Configuration()
    conf.set("mongo.input.uri" , "mongodb://127.0.0.1/spark_workshop/tags.input")
    conf.set("mongo.output.uri", "mongodb://127.0.0.1/spark_workshop/tags.output")

    //Create tag -> cont unique tuples and save them to Mongo
    sc.textFile(Conf.NAMES_PATH).map(name => (name, 1)).reduceByKey(_ + _).map(counter => {
      val obj = new BasicBSONObject()
      obj.put("count", counter._2)
      obj.put("tag", counter._1)
      (null, obj)
    }).saveAsNewAPIHadoopFile("file:///tags", classOf[Any], classOf[Any], classOf[com.mongodb.hadoop.MongoOutputFormat[Any, Any]], conf)

    //Read counters from Mongo and find the 5 most poplar tags
    val top = sc.newAPIHadoopRDD(conf, classOf[com.mongodb.hadoop.MongoInputFormat], classOf[Object], classOf[BSONObject])
      .map{case (id, bson) => (bson.get("count").toString.toInt, bson.get("tag").toString)}
      .sortBy(_._1, false).collect()
      .take(5)

    println(top)
  }

}
