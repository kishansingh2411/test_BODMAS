package ES

import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.elasticsearch.spark.rdd.EsSpark
import org.elasticsearch.spark.sql._

object ElasticSearch extends Serializable {

  def query(queryString : String, indexName : String, sqlContext :SQLContext) : DataFrame = {

    var result : DataFrame= null
    //try {
      result = sqlContext.read.format("org.elasticsearch.spark.sql")
        .option("es.nodes", "localhost")
        .option("es.port", "9200")
        .option("es.nodes.wan.only", "true")
        .option("es.mapping.id", "true")
       // .option("es.read.field.as.array.include", config.es_masterfile_read_field_as_array_include)
        .option("es.query", queryString)
        .load(indexName)

    result
  }

  def upsert(sc : SparkContext,query : String, indexName : String) : Unit = {
    var tmpRdd = sc.parallelize(Seq(query))
    EsSpark.saveJsonToEs(tmpRdd,indexName,Map[String,String]("es.mapping.id"->"pupId"))
  }
}
