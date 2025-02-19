package io.prophecy.pipelines.dq_bug_test.graph

import io.prophecy.libs._
import io.prophecy.pipelines.dq_bug_test.functions.PipelineInitCode._
import io.prophecy.pipelines.dq_bug_test.functions.UDFs._
import io.prophecy.pipelines.dq_bug_test.config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Limit_1 {
  def apply(context: Context, in: DataFrame): DataFrame = in.limit(10)
}
