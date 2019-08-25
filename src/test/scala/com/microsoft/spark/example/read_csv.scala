package com.microsoft.spark.example

import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.IntegerType

object read_csv {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("readcsv")
      .setMaster("local[*]")
    val sparkContext = new org.apache.spark.SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sparkContext)

  /*  val readinput = sqlContext.read.format("csv").option("header","true").load("/home/kishan/Git_Projects/csvdata/test.csv")
    readinput.printSchema()
    val f = readinput.withColumn("spend",readinput("spend").cast(IntegerType))
    f.printSchema()

   val r3 =  f.groupBy("cid").avg("spend")
    r3.show(false)
    f.show(false)*/

    def mapToTuple(line: String): (Int, (Float, Int)) = {
      val fields = line.split(',')
      return (fields(0).toInt, (fields(3).toFloat, 1))
    }


    val r1 = sparkContext.textFile("/home/kishan/Git_Projects/csvdata/test.csv").toJavaRDD().rdd
  //  val r2 = r1.map(_.split(",").map(x=>x.trim())).groupBy(x=>x(0))
    val h = r1.first()
    val r3 = r1.filter(x=>x!=h)
    val r4 = r3.map(x=>mapToTuple(x)).reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2)).map(x=>(x._1,x._2._1/x._2._2))
    r4.foreach(println(_))


   // val r3 = r1.map(mapToTuple())









  }
}
