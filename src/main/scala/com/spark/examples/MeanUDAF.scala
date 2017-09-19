package com.spark.examples

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.DoubleType
import com.spark.customUDAF._

object MeanUDAF {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder
      .appName("UDAF")
      .master("local")
      .getOrCreate()
    val sc = spark.sparkContext
    val sqlContext = spark.sqlContext

    val df = spark.read.option("header", "true").csv("data/FL_insurance_sample.csv")
      .withColumn("fr_site_limit", col("fr_site_limit").cast(DoubleType))
    df.printSchema()
    df.show()
    sqlContext.udf.register("mean", new Mean)
    val mean = new Mean
    df.agg(mean(col("fr_site_limit"))).show()
  }

}
