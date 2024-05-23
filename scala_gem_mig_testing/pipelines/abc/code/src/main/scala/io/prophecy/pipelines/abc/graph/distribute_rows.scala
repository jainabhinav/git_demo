package io.prophecy.pipelines.abc.graph

import io.prophecy.libs._
import io.prophecy.pipelines.abc.udfs.PipelineInitCode._
import io.prophecy.pipelines.abc.udfs.UDFs._
import io.prophecy.pipelines.abc.config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object distribute_rows {

  def apply(context: Context, in: DataFrame): (DataFrame, DataFrame) =
    (in.filter(lit(false)), in.filter(lit(true)))

}
