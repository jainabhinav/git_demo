package io.prophecy.pipelines.customer_orders

import io.prophecy.libs._
import io.prophecy.pipelines.customer_orders.config.Context
import io.prophecy.pipelines.customer_orders.config._
import io.prophecy.pipelines.customer_orders.udfs.UDFs._
import io.prophecy.pipelines.customer_orders.udfs._
import io.prophecy.pipelines.customer_orders.udfs.PipelineInitCode._
import io.prophecy.pipelines.customer_orders.graph._
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
      .newSession()
    val context = Context(spark, config)
    spark.conf
      .set("prophecy.metadata.pipeline.uri", "pipelines/customer_orders")
    registerUDFs(spark)
    try MetricsCollector.start(spark,
                               "pipelines/customer_orders",
                               context.config
    )
    catch {
      case _: Throwable =>
        MetricsCollector.start(spark, "pipelines/customer_orders")
    }
    apply(context)
    MetricsCollector.end(spark)
  }

}
