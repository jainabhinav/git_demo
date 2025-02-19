package io.prophecy.pipelines.redshift_test.graph

import io.prophecy.libs._
import io.prophecy.pipelines.redshift_test.config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object asd {

  def apply(context: Context): DataFrame = {
    val spark  = context.spark
    val Config = context.config
    import spark.implicits._
    var reader = spark.read.format("com.databricks.spark.redshift")
    reader = reader
      .option("forward_spark_s3_credentials", true)
      .option("tempDir",                      "dsgcb")
      .option("url",                          "jdbc:redshift://sdf:22/vbn")
      .option("user",                         s"${Config.abc}")
      .option("password",                     s"${Config.abc}")
    reader.load()
  }

}
