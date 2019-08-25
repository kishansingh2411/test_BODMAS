package com.microsoft.spark.example

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.avro._

case class Person(name: String, age: Int, id: Byte)

object sample_avro {
  def main(args: Array[String]): Unit = {


    val conf = new SparkConf().setAppName("converttoParquet").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val sqlContext   =new org.apache.spark.sql.SQLContext(sc)

    import sqlContext.implicits._
    val personDS = Seq(Person("Max", 33,1), Person("Adam", 32,2), Person("Muller", 62,3)).toDF()
   // personDS.show()
    personDS.printSchema()
    //personDS.write.format("com.databricks.spark.avro").save("/home/kishan/Git_Projects/csvdata/avrodata")

val newdf = sqlContext.read.format("com.databricks.spark.avro").load("/home/kishan/Git_Projects/csvdata/avrodata/*")
newdf.printSchema()
    newdf.show()
  }
}
