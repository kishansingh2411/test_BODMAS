package com.microsoft.spark.example

import org.apache.spark.SparkConf

object testhiveupdate {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("match_schema")
      .setMaster("local[*]")
    val sparkContext = new org.apache.spark.SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sparkContext)

    import sqlContext.implicits._


    val dfcustomer = sqlContext.read.format("csv").option("delimiter",",").option("header","true").load("/home/kishan/Git_Projects/csvdata/customer.csv")
      .toDF("id1","name1","address1")
    val dfupdate = sqlContext.read.format("csv").option("delimiter",",").load("/home/kishan/Git_Projects/csvdata/update.csv")
      .toDF("id","name","address")
    dfcustomer.registerTempTable("i")
    dfupdate.registerTempTable("u")
    val dfold = dfcustomer.join(dfupdate,dfcustomer("id1")===dfupdate("id"),"left_outer").select("id1","name1","address1").where("id is null")
    val dffinal = dfold.union(dfupdate)
    dffinal.show()

  }

}

