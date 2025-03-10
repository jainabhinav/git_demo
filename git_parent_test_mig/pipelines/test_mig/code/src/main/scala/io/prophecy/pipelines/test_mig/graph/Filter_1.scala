package io.prophecy.pipelines.test_mig.graph

import io.prophecy.libs._
import io.prophecy.pipelines.test_mig.functions.PipelineInitCode._
import io.prophecy.pipelines.test_mig.functions.UDFs._
import io.prophecy.pipelines.test_mig.config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Filter_1 {
  def apply(context: Context, in: DataFrame): DataFrame = in.filter(lit(true))
}
