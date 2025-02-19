package io.prophecy.pipelines.redshift_test.graph

import io.prophecy.libs._
import io.prophecy.pipelines.redshift_test.functions.PipelineInitCode._
import io.prophecy.pipelines.redshift_test.graph.Subgraph_1.config._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._
package object Subgraph_1 {
  def apply(context: Context, in0: DataFrame): Unit = {}
}
