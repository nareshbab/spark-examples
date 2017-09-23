package com.spark.examples

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
    val df = spark.read.option("header", "false").csv("/Users/naresh/Downloads/star2002-full.csv").limit(10)
    val listDF = df.groupBy("_c0").agg(collect_list("_c2").as("derived_column"))
    listDF.show()
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
    exploded_df.show()

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
