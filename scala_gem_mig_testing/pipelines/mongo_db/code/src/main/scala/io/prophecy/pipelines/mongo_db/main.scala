package io.prophecy.pipelines.mongo_db

import io.prophecy.libs._
import io.prophecy.pipelines.mongo_db.config._
import io.prophecy.pipelines.mongo_db.functions.UDFs._
import io.prophecy.pipelines.mongo_db.functions.PipelineInitCode._
import io.prophecy.pipelines.mongo_db.graph._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Main {

  def apply(context: Context): Unit = {
    val df_customers = customers(context)
  }

  def main(args: Array[String]): Unit = {
    val config = ConfigurationFactoryImpl.getConfig(args)
    val spark: SparkSession = SparkSession
      .builder()
      .appName("mongo_db")
      .config("spark.default.parallelism",             "4")
      .config("spark.sql.legacy.allowUntypedScalaUDF", "true")
      .enableHiveSupport()
      .getOrCreate()
    val context = Context(spark, config)
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/mongo_db")
    registerUDFs(spark)
    MetricsCollector.instrument(spark, "pipelines/mongo_db") {
      apply(context)
    }
  }

}
