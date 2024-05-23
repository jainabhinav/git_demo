package io.prophecy.pipelines.abc.graph

import io.prophecy.libs._
import io.prophecy.pipelines.abc.config.Context
import io.prophecy.pipelines.abc.udfs.UDFs._
import io.prophecy.pipelines.abc.udfs.PipelineInitCode._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object generate_range_dataframe_1 {
  def apply(context: Context): DataFrame = {
    val spark = context.spark
    val Config = context.config
    val out0 = spark.range(10).toDF("a")
    out0
  }

}
