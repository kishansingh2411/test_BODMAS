package com.microsoft.spark.example

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.elasticsearch.spark.rdd.EsSpark

object testES extends Serializable {
  def query(queryString : String, indexName : String, sqlContext :SQLContext) : DataFrame = {

    var result : DataFrame= null
    result = sqlContext.read.format("org.elasticsearch.spark.sql")
      .option("es.nodes", "localhost")
      .option("es.port", "9200")
      .option("es.nodes.wan.only", "true")
      .option("es.mapping.id", "true")
      .option("es.query", queryString)
      .load(indexName)
    result
  }

  def upsert(sc : SparkContext,query : String, indexName : String) : Unit = {
    var tmpRdd = sc.parallelize(Seq(query))
    EsSpark.saveJsonToEs(tmpRdd,indexName,Map[String,String]("es.mapping.id"->"pupId"))
  }


  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("sample_ES").setMaster("local[*]")
      //.set("es.write.operation", "upsert")
      .set("es.port","9200")
      .set("es.index.auto.create", "true")
      .set("es.nodes","localhost" )
      .set("pushdown", "true")
      //.set("es.mapping.id", "true")
      .set("es.read.field.as.array.include","nested.bar:3")

    val sc = new org.apache.spark.SparkContext(conf)
    val sqlContext=new SQLContext(sc)

    /*def esQueryBuilder(field : String, fieldValue : String) : String= {
      val query =
        """{
            "query": {
                "match_phrase": {
                   """"+field+"\": \""+fieldValue+""""
                }
            }
        }"""

      //      println("ES query "+query)

      query
    }*/

    /*val esquery = esQueryBuilder("first_name", "Harry")
    println("esquey ===",esquery)
    var r:DataFrame=null
    var df:DataFrame=null
   // r=query(esquery,"peopledetails/alldetails",sqlContext)
    df = sqlContext.read.format("org.elasticsearch.spark.sql")
     .option("query", esquery)
     .option("pushdown", "true")
     .load("peopledetails/alldetails")
    df.show(false)
*/

    // Elastic earch write ****************************************

    val input = sqlContext.read.parquet("/home/kishan/Downloads/userdata1.parquet")
    input.write.format("org.elasticsearch.spark.sql").mode("append").save("peopletest/typetest")
    //input.show(false)

  }
}
