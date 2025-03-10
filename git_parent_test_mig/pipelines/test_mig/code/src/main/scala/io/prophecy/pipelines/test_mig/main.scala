package io.prophecy.pipelines.test_mig

import io.prophecy.libs._
import io.prophecy.pipelines.test_mig.config._
import io.prophecy.pipelines.test_mig.functions.UDFs._
import io.prophecy.pipelines.test_mig.functions.PipelineInitCode._
import io.prophecy.pipelines.test_mig.graph._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Main {

  def apply(context: Context): Unit = {
    val df_csv_proj   = csv_proj(context)
    val df_csv_basics = csv_basics(context)
    val df_Filter_2   = Filter_2(context, df_csv_basics)
    val df_Filter_1   = Filter_1(context, df_csv_proj)
  }

  def main(args: Array[String]): Unit = {
    val config = ConfigurationFactoryImpl.getConfig(args)
    val spark: SparkSession = SparkSession
      .builder()
      .appName("test_mig")
      .enableHiveSupport()
      .getOrCreate()
    val context = Context(spark, config)
    spark.conf.set("prophecy.metadata.pipeline.uri",        "pipelines/test_mig")
    spark.conf.set("spark.default.parallelism",             "4")
    spark.conf.set("spark.sql.legacy.allowUntypedScalaUDF", "true")
    registerUDFs(spark)
    MetricsCollector.instrument(spark, "pipelines/test_mig") {
      apply(context)
    }
  }

}
