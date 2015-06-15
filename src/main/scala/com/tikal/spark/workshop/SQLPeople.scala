package com.tikal.spark.workshop

import org.apache.spark.SparkContext

/**
 * SQLPeople
 *
 * @author Dmitri Krasnenko
 */

case class Person(name: String, age: Int, kids: Int)

object SQLPeople {

  def main(args: Array[String]) {
    val TABLE_NAME = "people"

    //Create SQL context
    val sqlContext = new org.apache.spark.sql.SQLContext(new SparkContext("local[4]", "words-sql"))

    //Read JSON files
    val people = sqlContext.read.json(Conf.NAMES_JSON_PATH)

    //Register SQL table for running pure SQL queries
    people.registerTempTable(TABLE_NAME)

    //SELECT * FROM people
    people.select("*").show()
    sqlContext.sql(s"SELECT * FROM $TABLE_NAME").show()

    //SELECT name FROM people WHERE age >= 30
    people.filter(people("age") >= 30).select("name").show()
    sqlContext.sql(s"SELECT name FROM $TABLE_NAME WHERE age >= 30").show()

    //SELECT sum(age),sum(kids) FROM people
    people.agg("age" ->"sum","kids"->"sum").show()
    sqlContext.sql(s"SELECT sum(age) as age_sum, sum(kids) as kids_sum FROM $TABLE_NAME").show()
  }
}

object SQLPerson {

  def main(args: Array[String]) {

    val TABLE_NAME = "person"

    val sc = new SparkContext(
      "local[4]",
      "person-sql"
    )

    val sqlContext = new org.apache.spark.sql.SQLContext(
      sc
    )

    //Import implicit RDD-2-DataFrame conversion functions
    import sqlContext.implicits._

    val person = sc.textFile(Conf.NAMES_RAWS_PATH)
      .map(_.split(","))
      .map(p => Person(p(0), p(1).trim.toInt, p(2).trim.toInt))
      .toDF()

    person.registerTempTable(TABLE_NAME)

    person.agg(
      "age" ->"sum","kids"->"sum").show()

    sqlContext.sql(
      s"SELECT sum(age) as age_sum, sum(kids) as kids_sum FROM $TABLE_NAME").write.format("json").save(s"${Conf.SPARK_DATA}/person")
  }
}
