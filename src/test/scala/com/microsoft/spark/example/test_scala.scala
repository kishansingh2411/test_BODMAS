package com.microsoft.spark.example
import org.apache.spark._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.IntegerType
object test_scala {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("readcsv")
      .setMaster("local[*]")
    val sparkContext = new org.apache.spark.SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sparkContext)

    import sqlContext.implicits._

    /* println(fabgen(5))

     def fabgen(n:Int) :Int = {

       def febgentmp(n:Int,a:Int, b:Int):Int = n match {

         case 0 => a
         case _ => febgentmp(n-1, b ,a+b)

       }

 return febgentmp(n,0,1)
     }*/

    val df = sqlContext.read.format("csv").option("header","false").load("/home/kishan/Git_Projects/csvdata/newdoc.csv").toDF("id","Name","key")
    var allid=df.select("Id").distinct().toJSON.toJavaRDD
    var dfuniq = df.select("id","key")
    dfuniq=dfuniq.groupBy("key").agg(Map("id"->"collect_set")).withColumnRenamed("collect_set(id)","IdList")//
    .withColumn("size_IdList",size(col("IdList")))
   var duplicate = dfuniq.where("size_IdList>1 ").withColumn("Id",explode(col("IdList"))).select("Id","key").distinct.toJSON.toJavaRDD
   var nonduplicate = allid.subtract(duplicate)
    var nonduplicatedf = sqlContext.read.json(nonduplicate)
    var duplicatedf = sqlContext.read.json(duplicate)
    duplicatedf.show(false)

  }
}
