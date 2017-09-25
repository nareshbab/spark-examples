name := "spark-examples"

version := "0.1"

scalaVersion := "2.10.5"


libraryDependencies ++= Seq("org.apache.spark" % "spark-sql_2.10" % "2.2.0",
  "org.apache.spark" % "spark-core_2.10" % "2.2.0",
  "org.apache.spark" % "spark-mllib_2.10" % "2.2.0",
  "org.apache.spark" % "spark-streaming_2.10" % "2.2.0",
  "org.apache.spark" % "spark-graphx_2.10" % "2.2.0",
  "org.mongodb" % "casbah_2.10" % "3.1.1" pomOnly(),
  "ai.h2o" % "h2o-core" % "3.14.0.2" % "runtime" pomOnly()

)
