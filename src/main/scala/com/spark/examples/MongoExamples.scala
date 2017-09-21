package com.spark.examples

import org.apache.spark.sql.SparkSession
import com.mongodb.casbah.MongoClient
import com.mongodb.casbah.MongoClientURI
import com.mongodb.casbah.commons.MongoDBObject


object MongoExamples {


  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder
      .appName("UDAF")
      .master("local")
      .getOrCreate()
    val sc = spark.sparkContext
    val sqlContext = spark.sqlContext
    val final_map = Map("procedure_code"->Map("1" -> 3, "2" -> 5))
    val mongo = MongoClient(MongoClientURI("mongodb://127.0.0.1:27017"))
    val db = mongo("spark")
    val collection = db("codes")
    val mongoObject = MongoDBObject("codes" -> final_map)
    collection.insert(mongoObject)
    mongo.close()


  }




}
