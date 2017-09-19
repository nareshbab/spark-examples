package com.spark.customUDAF

//Project imports
import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.MutableAggregationBuffer
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction
import org.apache.spark.sql.types._

class Mean extends UserDefinedAggregateFunction{

  //input fields for custom aggregation
  override def inputSchema: StructType =
    StructType(StructField("value", DoubleType) :: Nil)

  //internal fields for computing aggregate
  override def bufferSchema: StructType = StructType(
    StructField("sum", DoubleType) ::
    StructField("count", LongType) :: Nil
  )

  //output datatype
  override def dataType: DataType = DoubleType

  override def deterministic: Boolean = true

  //initial value for buffer schema
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = 0.0
    buffer(1) = 0L
  }

  //update buffer based on rows
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    buffer(0) = buffer.getDouble(0) + input.getDouble(0)
    buffer(1) = buffer.getLong(1) + 1
  }

  //merge two buffer schema types
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) = buffer1.getDouble(0) + buffer2.getDouble(0)
    buffer1(1) = buffer1.getLong(1) + buffer2.getLong(1)
  }

  override def evaluate(buffer: Row): Any = {
    buffer.getDouble(0)/buffer.getLong(1)
  }
}
