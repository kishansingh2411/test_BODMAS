package com.microsoft.spark.example

import org.apache.spark.SparkConf

object dataskew {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("skewdatacase")
      .setMaster("local[*]")
    val sparkContext = new org.apache.spark.SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sparkContext)


    val df1 = sqlContext.read.format("csv").option("delimiter",",").option("header","true").load("/home/kishan/Git_Projects/csvdata/customer.csv")
      .toDF("id","name","address")

    val dfnew = sqlContext.read.format("csv").option("delimiter",",").option("header","true").load("/home/kishan/Git_Projects/csvdata/sales_doc.csv")
      .toDF("idnew","tid","amount")

    val df3 = df1.join(dfnew, df1("id") === dfnew("idnew"),"Inner").select("id","name","address","amount")


    df3.createOrReplaceTempView("df3")
    sqlContext.sql("select id, Floor(Rand(123456)*19) from df3").show(false)
    sqlContext.sql("select id,explode(array(0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19)) from df3").show(false)
  }
}
