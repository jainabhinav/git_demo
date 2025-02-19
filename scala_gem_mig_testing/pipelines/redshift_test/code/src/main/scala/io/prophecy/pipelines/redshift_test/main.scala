package io.prophecy.pipelines.redshift_test

import io.prophecy.libs._
import io.prophecy.pipelines.redshift_test.config._
import io.prophecy.pipelines.redshift_test.functions.UDFs._
import io.prophecy.pipelines.redshift_test.functions.PipelineInitCode._
import io.prophecy.pipelines.redshift_test.graph._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Main {

  def apply(context: Context): Unit = {
    val df_redshift_old = redshift_old(context)
    val df_Subgraph_1 = Subgraph_1.apply(
      Subgraph_1.config.Context(context.spark, context.config.Subgraph_1)
    )
    val df_asd = asd(context)
  }

  def main(args: Array[String]): Unit = {
    val config = ConfigurationFactoryImpl.getConfig(args)
    val spark: SparkSession = SparkSession
      .builder()
      .appName("redshift_test")
      .config("spark.default.parallelism",             "4")
      .config("spark.sql.legacy.allowUntypedScalaUDF", "true")
      .enableHiveSupport()
      .getOrCreate()
    val context = Context(spark, config)
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/redshift_test")
    registerUDFs(spark)
    MetricsCollector.instrument(spark, "pipelines/redshift_test") {
      apply(context)
    }
  }

}
