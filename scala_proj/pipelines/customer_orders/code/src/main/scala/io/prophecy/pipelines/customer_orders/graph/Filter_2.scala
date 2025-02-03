package io.prophecy.pipelines.customer_orders.graph

import io.prophecy.libs._
import io.prophecy.pipelines.customer_orders.udfs.PipelineInitCode._
import io.prophecy.pipelines.customer_orders.udfs.UDFs._
import io.prophecy.pipelines.customer_orders.config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Filter_2 {
  def apply(context: Context, in: DataFrame): DataFrame = in.filter(lit(true))
}
