package com.spark
import java.util.{Properties, StringJoiner}

import scala.collection.mutable
import org.apache.spark.SparkConf
import test_new_jdk.test_BODMAS._
import scala.collection.mutable.ArrayBuffer
import com.typesafe.config.ConfigFactory
import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import update_sql_table._
object implementformula    {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("test_connection").master("local[*]").getOrCreate()
    val sql_conf = ConfigFactory.load("application.conf")
    val connectionProperties = new Properties()
    connectionProperties.put("user",sql_conf.getString("sql.details.username"))
    connectionProperties.put("password",sql_conf.getString("sql.details.password"))
    connectionProperties.put("driver",sql_conf.getString("sql.details.driver"))

    var df_tbl_acc_dtl = spark.read.jdbc(sql_conf.getString("sql.details.url"),sql_conf.getString("sql.details.tbl_acc_dtl"),connectionProperties)
    var df_tbl_acc_ben = spark.read.jdbc(sql_conf.getString("sql.details.url"),sql_conf.getString("sql.details.tbl_acc_ben"),connectionProperties)
    var df_tbl_new_mdl = spark.read.jdbc(sql_conf.getString("sql.details.url"),sql_conf.getString("sql.details.tbl_new_mdl"),connectionProperties)

    df_tbl_acc_ben.createOrReplaceTempView("account_benefit")

      def getMapFromSql(tableName: String): RDD[(String, String)]={
      val mapRdd = spark.read.jdbc(sql_conf.getString("sql.details.url"),tableName,connectionProperties).rdd
      val schemaMap = mapRdd.map(x => ((x.getAs[String]("tbl_col_name").toLowerCase), x.getAs[String]("df_col_name")))
      schemaMap
    }

     val schemaMap = getMapFromSql(sql_conf.getString("sql.details.tbl_schema_map")).collectAsMap()
     var formula = df_tbl_new_mdl.select("formula","name").where("execution_type == 'new'" ).collectAsList()

    for(i<- 0 until(formula.size())){
      var cleanformula = (formula.get(i)).mkString(",").split(",")(0).replaceAll("[\\[\\]]","")
      var formula_name = (formula.get(i)).mkString(",").split(",")(1).replaceAll("[\\[\\]]","")
      schemaMap.foreach(x=>{
        cleanformula = cleanformula.replaceAll(x._1,x._2)
      })
      df_tbl_acc_ben = spark.sql(s"select *, ($cleanformula) as $formula_name from account_benefit ")
      df_tbl_acc_ben.createOrReplaceTempView("account_benefit")

      val query = "update"+ " " +sql_conf.getString("sql.details.tbl_new_mdl")+" "+ "set execution_type='completed' where name = "+ "'"+formula_name+"'"
      update_sql_table.update_sql(query)

    }

    df_tbl_acc_ben.show(false)
 /*   def findstr (df:DataFrame,str:String):DataFrame={
     var formula = df.select("formula").where("execution_type == 'new'" )

    }*/

  // var str =  "kishan.account_benefit.benefit_amount + kishan.account_benefit.benefit_amount * (kishan.account_benefit.benefit_amount - 5)"

   /* df_tbl_acc_ben.createOrReplaceTempView("account_benefit")
    spark.sql(s"select ($str) as sum from account_benefit ").show(false)*/
   // spark.sql(s"select (account_benefit.benefit_amount + account_benefit.benefit_amount * (account_benefit.benefit_amount - 5)) as s from account_benefit ").show(false)
  //  df_tbl_acc_dtl.withColumn("newcol",newudf(lit("30 * (81 - 51) + 12 - 18 * ((21 - 12)/3)"))).show(false)
    /* BroadCasting Maps*/
    /*val schemaMap = getMapFromSql(sql_conf.getString("sql.details.tbl_schema_map"))
    df_tbl_acc_ben.withColumn("necol",matchdf(df_tbl_acc_ben("benefit_amount"))).show(false)*/
  }
}
