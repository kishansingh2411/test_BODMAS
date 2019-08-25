package com.microsoft.spark.example

import org.apache.spark.SparkConf
import kafka.serializer.StringDecoder
import org.apache.spark.streaming
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import utils.MapperUtil
import org.elasticsearch.spark.sql._
import mongoDB.mongoreadwrite



object Kafka_streaming {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .set("spark.mongodb.input.uri", "mongodb://mongo-admin:password@localhost:27017/admin?authSource=admin?readPreference=secondaryPreferred")
      .set("spark.mongodb.output.uri", "mongodb://mongo-admin:password@localhost:27017/admin?authSource=admin")
      .set("spark.mongodb.output.collection","peoplestramdata" )
      .set("spark.mongodb.output.database", "peopledata")
      .setAppName("Streaming")
    .setMaster("local[*]")
    val sparkContext = new org.apache.spark.SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sparkContext)

    val functionTOCreateContext: () => StreamingContext = () => {
      conf.set("spark.streaming.receiver.writeAheadLog.enable", "true")
      val ssc = new org.apache.spark.streaming.StreamingContext(sparkContext, Seconds(5))
      //ssc.checkpoint(checkPointDirName)
      val topicsSet = "test".split(",").toSet
      val kafkaParams = Map[String, String](
        "metadata.broker.list" -> "localhost:9092",
        "auto.offset.reset" -> "smallest",
        "enable.auto.commit" -> "true")

      val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicsSet)

      messages.foreachRDD { rdd =>
        if (rdd != null && !rdd.isEmpty()) {
          val sqlContext = org.apache.spark.sql.SQLContext.getOrCreate(rdd.context)

          var filterDF = sqlContext.read.json(rdd.map(single => single._2.trim))
                /* filterDF.show(false)*/
                val rows = filterDF.select("_corrupt_record").collect().map(_.getString(0)).mkString(" ")
          val query_n="?q="+rows
          val esresult = sqlContext.esDF("peopledetails/alldetails",query_n)
          //val esresult = sqlContext.esDF("peopledetails/alldetails","?q=Kathy")
        //  esresult.printSchema()

         // val resultDF = esresult.select("first_name","last_name","email")//.show(100,false)
          esresult.show(false)
          val resultDF = esresult
          mongoreadwrite.mongowrite("peoplestramdata",resultDF)

        }
      }
      ssc
    }


    val ssc = StreamingContext.getActiveOrCreate("/home/kishan/Documents", functionTOCreateContext, createOnError = true)
    //val ssc = StreamingContext.getActiveOrCreate(checkPointDirName, functionTOCreateContext, createOnError = true)

    ssc.start()
    ssc.awaitTermination()
  }
}
