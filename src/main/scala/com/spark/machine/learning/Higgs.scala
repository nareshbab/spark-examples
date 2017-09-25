package com.spark.machine.learning

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.mllib
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg._
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.mllib.evaluation._
import org.apache.spark.mllib.tree._
import org.apache.spark.mllib.tree.model._


object Higgs {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("Higgs")
      .getOrCreate()
    val sqlContext = spark.sqlContext

    val rawData = spark.sparkContext.textFile(s"${sys.env.get("DATADIR").getOrElse("data")}/higgs100k.csv")
    println(s"Number of rows: ${rawData.count}")
    val data = rawData.map(line => line.split(",").map(_.toDouble))
    val response: RDD[Int] = data.map(row => row(0).toInt)
    val features: RDD[Vector] = data.map(line => Vectors.dense(line.slice(1, line.size)))
    val featuresMatrix = new RowMatrix(features)
    val featuresSummary = featuresMatrix.computeColumnSummaryStatistics()

    val nonZeros = featuresSummary.numNonzeros
    val numRows = featuresMatrix.numRows
    val numCols = featuresMatrix.numCols
    val colsWithZeros = "c"


  }

}
