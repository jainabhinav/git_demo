package io.prophecy.pipelines.report_top_customers.graph

import io.prophecy.libs._
import io.prophecy.pipelines.report_top_customers.config.Context
import io.prophecy.pipelines.report_top_customers.udfs.UDFs._
import io.prophecy.pipelines.report_top_customers.udfs.PipelineInitCode._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object update_flow_flag {
  def apply(context: Context): DataFrame = {
    val spark = context.spark
    val Config = context.config
    print(Config.flow_flag)
    Config.flow_flag = "lower" // write your logic here
    print(Config.flow_flag)
    
    val out0 = spark.sql("select 1 as a")
    out0
  }

}
