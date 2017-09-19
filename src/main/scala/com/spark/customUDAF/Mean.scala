package com.spark.customUDAF

//Project imports
import org.apache.spark.sql.expressions.MutableAggregationBuffer
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction
import org.apache.spark.sql.types.{DoubleType, StructField, StructType}

class Mean extends UserDefinedAggregateFunction{

  override def inputSchema: StructType =
    StructType(StructField("doubleCol", DoubleType) :: Nil)

  over

}
