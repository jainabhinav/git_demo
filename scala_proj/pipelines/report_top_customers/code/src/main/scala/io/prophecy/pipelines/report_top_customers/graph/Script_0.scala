package io.prophecy.pipelines.report_top_customers.graph

import io.prophecy.libs._
import io.prophecy.pipelines.report_top_customers.config.Context
import io.prophecy.pipelines.report_top_customers.udfs.UDFs._
import io.prophecy.pipelines.report_top_customers.udfs._
import io.prophecy.pipelines.report_top_customers.udfs.PipelineInitCode._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Script_0 {
  def apply(context: Context): Unit = {
    val spark = context.spark
    val Config = context.config
    println("report_top_customers pipeline exectuion")
  }

}
