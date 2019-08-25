package com.microsoft.spark.example

import org.apache.spark._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.IntegerType

object match_schema {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("match_schema")
      .setMaster("local[*]")
    val sparkContext = new org.apache.spark.SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sparkContext)

    import sqlContext.implicits._

   // val r1 = sparkContext.textFile("/home/kishan/Git_Projects/csvdata/pipedata.csv")//.toJavaRDD().rdd
    // val r2 = r1.map(_.split("|")).map(x=>(x(0),x(1),x(2),x(3),x(4),x(5),x(6)))//.toDF()
    //r2.foreach(println)

    val df1 = sqlContext.read.format("csv").option("delimiter","|").load("/home/kishan/Git_Projects/csvdata/pipedata.csv")
     .toDF("id","name","id2","address","pid","city","country")

    val dfnew = sqlContext.read.format("csv").option("delimiter",",").load("/home/kishan/Git_Projects/csvdata/test.csv")
      .toDF("cid","pid","name","spend")

    var df2 = df1.columns.toList
    var df3 = dfnew.columns.toList

    df2.foreach(println)
    df3.foreach(println)




   // var df4 = df3.diff(df2)
    

    //println(df4.length)


  }

}
