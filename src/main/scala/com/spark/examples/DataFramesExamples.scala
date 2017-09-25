package com.spark.examples

import com.mongodb.casbah.{MongoClient, MongoClientURI}
import com.mongodb.casbah.commons.MongoDBObject
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions._

object DataFramesExamples {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("df")
      .master("local[*]")
      .getOrCreate()

    val sc = spark.sparkContext
    val sqlContext = spark.sqlContext
    import spark.implicits._
    val df = spark.read.option("header", "false").csv("/Users/naresh/Downloads/star2002-full.csv").limit(1000)
    val listDF = df.groupBy("_c0").agg(collect_list("_c2").as("derived_column"))
//    listDF.show()
//      +---+--------------------+
//      |_c0|      derived_column|
//      +---+--------------------+
//      |  3|[ 812,  816,  828...|
//      |  0|[ 815,  818,  819...|
//      |  5|  [ 811,  866,  875]|
//      |  1|[ 807,  808,  809...|
//      |  4|[ 810,  867,  872...|
//      |  2|[ 814,  832,  839...|
//      +---+--------------------+

    val exploded_df = listDF.withColumn("derived_column", explode($"derived_column"))
//    exploded_df.show()

//      +---+--------------+
//      |_c0|derived_column|
//      +---+--------------+
//      |  3|           812|
//      |  3|           816|
//      |  3|           828|
//      |  3|           844|
//      |  3|           851|
//      |  3|           870|
//      |  3|           878|
//      |  3|           879|
//      |  3|           881|
//      |  3|           884|
//      |  3|           897|
//      |  3|           898|
//      |  3|           901|
//      |  3|           903|
//      |  0|           815|
//      |  0|           818|
//      |  0|           819|
//      |  0|           821|
//      |  0|           822|
//      |  0|           823|
//      +---+--------------+

    val countDF = exploded_df.groupBy("_c0", "derived_column").count
    val facilityIndex = countDF.columns.indexOf("_c0")
    val codeIndex = countDF.columns.indexOf("derived_column")
    val countIndex = countDF.columns.indexOf("count")
    val maps = countDF.rdd.groupBy(_.getString(facilityIndex)).map{
      case (facility, values) => Map(facility -> values.map(v => ( v.getString(codeIndex), v.getLong(countIndex))).toMap)
    }.collect
//    val fcGroupBy = countDF.withColumn("st", struct($"derived_column", $"count"))
//    val finalDF = fcGroupBy.groupBy("_c0").agg(collect_list("st"))
//
//    val finalMap = finalDF.rdd.collect.map(r=> Map(r(0), r(1)):_*)
    val mongo = MongoClient(MongoClientURI("mongodb://127.0.0.1:27017"))
    val db = mongo("spark")
    val collection = db("new")
    val mongoObject = MongoDBObject("check" -> maps)
    collection.insert(mongoObject)
    mongo.close()

    //    +---+--------------+-----+--------+
//    |_c0|derived_column|count|      st|
//    +---+--------------+-----+--------+
//    |  1|           914|    1|[ 914,1]|
//    |  1|           747|    3|[ 747,3]|
//    |  2|           967|    1|[ 967,1]|
//    |  2|           711|    1|[ 711,1]|
//    |  3|           693|    1|[ 693,1]|
//    |  4|           787|    1|[ 787,1]|


    val structDF = countDF.withColumn("custom", struct($"derived_column", $"count")).select("_c0", "custom")
      .groupBy("_c0").agg(collect_list("custom").as("unmatchedCodesCount"))
    structDF.toJSON.show()

//      +---+--------------+-----+
//      |_c0|derived_column|count|
//      +---+--------------+-----+
//      |  2|           858|    1|
//      |  0|           834|    1|
//      |  3|           879|    1|
//      |  3|           881|    1|
//      |  4|           867|    1|
//      |  1|           817|    1|
//      |  0|           819|    1|


  }

}
