package io.prophecy.pipelines.dq_bug_test

import io.prophecy.libs._
import io.prophecy.pipelines.dq_bug_test.config._
import io.prophecy.pipelines.dq_bug_test.functions.UDFs._
import io.prophecy.pipelines.dq_bug_test.functions.PipelineInitCode._
import io.prophecy.pipelines.dq_bug_test.graph._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Main {

  def apply(context: Context): Unit = {
    val df_customers  = customers(context)
    val df_Filter_1   = Filter_1(context,   df_customers)
    val df_Reformat_1 = Reformat_1(context, df_Filter_1)
    val df_identity   = identity(context,   df_Reformat_1)
    val df_Limit_1    = Limit_1(context)
  }

  def main(args: Array[String]): Unit = {
    val config = ConfigurationFactoryImpl.getConfig(args)
    val spark: SparkSession = SparkSession
      .builder()
      .appName("DQ_bug_test")
      .config("spark.default.parallelism",             "4")
      .config("spark.sql.legacy.allowUntypedScalaUDF", "true")
      .enableHiveSupport()
      .getOrCreate()
    val context = Context(spark, config)
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/DQ_bug_test")
    registerUDFs(spark)
    MetricsCollector.instrument(spark, "pipelines/DQ_bug_test") {
      apply(context)
    }
  }

}
