package io.prophecy.pipelines.abc

import io.prophecy.libs._
import io.prophecy.pipelines.abc.config._
import io.prophecy.pipelines.abc.udfs.UDFs._
import io.prophecy.pipelines.abc.udfs.PipelineInitCode._
import io.prophecy.pipelines.abc.graph._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Main {

  def apply(context: Context): Unit = {
    val df_jdbc_query = jdbc_query(context)
  }

  def main(args: Array[String]): Unit = {
    val config = ConfigurationFactoryImpl.getConfig(args)
    val spark: SparkSession = SparkSession
      .builder()
      .appName("abc")
      .config("spark.default.parallelism",             "4")
      .config("spark.sql.legacy.allowUntypedScalaUDF", "true")
      .enableHiveSupport()
      .getOrCreate()
    val context = Context(spark, config)
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/abc")
    registerUDFs(spark)
    MetricsCollector.instrument(spark, "pipelines/abc") {
      apply(context)
    }
  }

}
