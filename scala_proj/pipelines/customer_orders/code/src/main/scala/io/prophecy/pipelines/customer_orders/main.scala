package io.prophecy.pipelines.customer_orders

import io.prophecy.libs._
import io.prophecy.pipelines.customer_orders.config._
import io.prophecy.pipelines.customer_orders.udfs.UDFs._
import io.prophecy.pipelines.customer_orders.udfs.PipelineInitCode._
import io.prophecy.pipelines.customer_orders.graph._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Main {

  def apply(context: Context): Unit = {
    val df_customers = customers(context)
    val df_Filter_2  = Filter_2(context, df_customers)
    val df_new_csv   = new_csv(context)
    val df_Filter_1  = Filter_1(context, df_new_csv)
    Script_0(context)
  }

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
      .set("prophecy.metadata.pipeline.uri", "pipelines/customer_orders")
    registerUDFs(spark)
    MetricsCollector.instrument(spark, "pipelines/customer_orders") {
      apply(context)
    }
  }

}
