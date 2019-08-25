package com.microsoft.spark.example



import java.text.SimpleDateFormat
import java.util.{Calendar, Date, Properties}

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.fasterxml.jackson.databind.node.{ArrayNode, ObjectNode}
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import utils.MapperUtil
import ES.ElasticSearch
import org.elasticsearch.spark.sql._

import scala.util.control.Breaks.{break, breakable}

object process extends Serializable {
  def profiles( sc : SparkContext, sqlContext: SQLContext , df : DataFrame)= {

    println("df.count " + df.count())
    import sqlContext.implicits._



    def esQueryBuilder(field: String, fieldValue: String): String = {
      val query =
        """{
            "query": {
                "match_phrase": {
                   """" + field + "\": \"" + fieldValue.replaceAll("\\W","")+
          """"
                }
            }
        }"""

      //      println("ES query "+query)

      query
    }

    val data = df.toJSON.collect()
    val rdd_new = df.rdd

    /*rdd_new.foreach{
      a=> {

       // val esquery = esQueryBuilder("first_name", a.toString())
val  ab = sqlContext.esDF("peopledetails/type1")

        ab.show(false)
      ab
        /*val esResult = elasticSearchQueryMethod(esquery,"peopledetails/type1")
        esResult.show(false)
        esResult*/
      }
    }*/



    //val esquery = esQueryBuilder("first_name", )

    def elasticSearchQueryMethod(esquery:String, indexname : String): DataFrame ={

      var esResult : DataFrame =  null
      esResult = ElasticSearch.query(esquery, indexname, sqlContext)
      esResult
    }
    //val esquery = esQueryBuilder("first_name", )



  }

  }
