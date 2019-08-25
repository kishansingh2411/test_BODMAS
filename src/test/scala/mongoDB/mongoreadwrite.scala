package mongoDB

import com.mongodb.spark._
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SQLContext}
import com.mongodb.spark._


object mongoreadwrite extends Serializable {

  val conf = new SparkConf()
    .set("spark.mongodb.input.uri", "mongodb://mongo-admin:password@localhost:27017/admin?authSource=admin?readPreference=secondaryPreferred")
    .set("spark.mongodb.output.uri", "mongodb://mongo-admin:password@localhost:27017/admin?authSource=admin")
    .set("spark.mongodb.output.collection","peoplestramdata" )
    .set("spark.mongodb.output.database", "peopledata")
   // .setAppName("Streaming")
   // .setMaster("local[*]")


  def mongowrite(collection:String,df:DataFrame) ={

    MongoSpark.save(df.write.option("spark.mongodb.output.database", "peopledata").option("collection", collection).mode("append"))
    println("written ",df.count()," records in database")

  }

  def main(args: Array[String]): Unit = {
    val sc = new org.apache.spark.SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    /*val conf = new SparkConf()
      .set("spark.mongodb.input.uri", "mongodb://mongo-admin:password@localhost:27017/admin?authSource=admin?readPreference=secondaryPreferred")
      .set("spark.mongodb.output.uri", "mongodb://mongo-admin:password@localhost:27017/admin?authSource=admin")
      .set("spark.mongodb.output.collection","OutputTest" )
      .set("spark.mongodb.output.database", "testdata")
      .setAppName("Streaming")
      .setMaster("local[*]")
    val sc = new org.apache.spark.SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)*/

    val input = sqlContext.read.parquet("/home/kishan/Downloads/userdata1.parquet")


    //MongoSpark.save(input.write.option("collection", "OutputTest").mode("overwrite"))
   mongowrite("OutputTest",input)

  }
}
