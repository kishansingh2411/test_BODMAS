package com.microsoft.spark.example

package DBService

//import configLoader.Settings
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.elasticsearch.spark._
import org.elasticsearch.spark.rdd.EsSpark

object ElasticSearch extends Serializable {

  //def query(queryString : String, indexName : String, sqlContext :SQLContext, config : Settings) : DataFrame = {
  def query(queryString : String, indexName : String, sqlContext :SQLContext) : DataFrame = {

    var result : DataFrame= null
    //try {
    result = sqlContext.read.format("org.elasticsearch.spark.sql")
      .option("es.nodes", "localhost")
      //.option("es.nodes", config.es_nodes)
      .option("es.port", "9200")
      //.option("es.port", config.es_port)
      //        .option("es.read.metadata", "true")
      .option("es.nodes.wan.only", "true")
      .option("es.mapping.id", "true")
      // .option("es.read.field.as.array.include", config.es_masterfile_read_field_as_array_include)
      .option("es.query", queryString)
      .load(indexName)
    //}catch {
    //case ex: Exception=>{
    // println("Missing file exception")
    // }}
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
      //.set("es.mapping.id", "true")
      .set("es.read.field.as.array.include","nested.bar:3")

    val sc = new org.apache.spark.SparkContext(conf)
    val sqlContext=new SQLContext(sc)

    def esQueryBuilder(field : String, fieldValue : String) : String= {
      val query1 =
        """{
            "query": {
                "match_phrase": {
                   """"+field+"\": \""+fieldValue+""""
                }
            }
        }"""

      //      println("ES query "+query)

      query1
    }

    val esquery = esQueryBuilder("first_name", "Harry")


    var r:DataFrame=null
    r=query(esquery,"peopledetails/alldetails",sqlContext)

    r.show(false)
  }
}
