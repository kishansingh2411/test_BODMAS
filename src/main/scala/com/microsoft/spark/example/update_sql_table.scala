package com.spark

import java.sql.DriverManager
import java.sql.Connection
import java.util.Properties

import com.typesafe.config.ConfigFactory


object update_sql_table {

  def main(args: Array[String]): Unit = {
    val sql ="update kishan.new_model set execution_type='new' where name='kishan_formula2'"
    update_sql_table.update_sql(sql)
  }

  def update_sql(query:String): Unit ={
    val sql_conf = ConfigFactory.load("application.conf")
    val connectionProperties = new Properties()
    connectionProperties.put("user",sql_conf.getString("sql.details.username"))
    connectionProperties.put("password",sql_conf.getString("sql.details.password"))
    connectionProperties.put("driver",sql_conf.getString("sql.details.driver"))
    var connection:Connection = null

    try {
      connection = DriverManager.getConnection(sql_conf.getString("sql.details.url"), sql_conf.getString("sql.details.username"), sql_conf.getString("sql.details.password"))
      val stmt = connection.createStatement
      val sql = query

      stmt.executeUpdate(sql)
    } catch {
      case ex:Exception => println(ex)
    } finally {
      connection.close()
    }
  }

}
