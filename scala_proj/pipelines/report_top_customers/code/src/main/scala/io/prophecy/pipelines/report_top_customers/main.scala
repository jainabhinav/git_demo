package io.prophecy.pipelines.report_top_customers

import io.prophecy.libs._
import io.prophecy.pipelines.report_top_customers.config._
import io.prophecy.pipelines.report_top_customers.udfs.UDFs._
import io.prophecy.pipelines.report_top_customers.udfs.PipelineInitCode._
import io.prophecy.pipelines.report_top_customers.graph._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Main {

  def apply(context: Context): Unit =
    Script_0(context)

  def main(args: Array[String]): Unit = {
    val config = ConfigurationFactoryImpl.getConfig(args)
    val spark: SparkSession = SparkSession
      .builder()
      .appName("Prophecy Pipeline")
      .config("spark.default.parallelism",             "4")
      .config("spark.sql.legacy.allowUntypedScalaUDF", "true")
      .enableHiveSupport()
      .getOrCreate()
    val context = Context(spark, config)
    spark.conf
      .set("prophecy.metadata.pipeline.uri", "pipelines/report_top_customers")
    registerUDFs(spark)
    MetricsCollector.instrument(spark, "pipelines/report_top_customers") {
      apply(context)
    }
  }

}
