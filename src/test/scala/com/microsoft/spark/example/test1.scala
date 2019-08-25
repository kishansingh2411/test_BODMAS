package com.microsoft.spark.example

import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
import org.elasticsearch.spark.sql._
import org.apache.spark.sql.SQLContext._

object test1 {
  def main(args: Array[String]): Unit = {

    /*val conf = new SparkConf().setAppName("sample_ES").setMaster("local[*]")
     //.set("es.write.operation", "upsert")
      .set("es.port","9200")
      .set("es.index.auto.create", "true")
      .set("es.nodes","localhost" )
      .set("pushdown", "true")
      //.set("es.mapping.id", "true")
      .set("es.read.field.as.array.include","nested.bar:3")

    val sc = new org.apache.spark.SparkContext(conf)
    val sqlContext=new SQLContext(sc)

    val df = sqlContext.read.format("org.elasticsearch.spark.sql").load("peopledetails/alldetails")
    df.show(false)*/
    val ipv4 = """^([01]?[0-9]?[0-9]|2[0-4][0-9]|25[0-5])\.([01]?[0-9]?[0-9]|2[0-4][0-9]|25[0-5])\.([01]?[0-9]?[0-9]|2[0-4][0-9]|25[0-5])\.([01]?[0-9]?[0-9]|2[0-4][0-9]|25[0-5])$""".r
    val ipv6 = """^[0-9abcdef]{1,4}\:[0-9abcdef]{1,4}\:[0-9abcdef]{1,4}\:[0-9abcdef]{1,4}\:[0-9abcdef]{1,4}\:[0-9abcdef]{1,4}\:[0-9abcdef]{1,4}\:[0-9abcdef]{1,4}$""".r

    var a = Array("121.18.19.20","0.12.12.34","121.234.12.12","23.45.12.56","0.1.2.3","2001:0db8:0000:0000:0000:ff00:0042:8329")
    for (x <- a )
      {
        println((ipv4 findFirstIn(x)).map(_ => "IPv4").getOrElse((ipv6 findFirstIn x).map(_ => "IPv6").getOrElse("Neither")))

      }

  }
}
