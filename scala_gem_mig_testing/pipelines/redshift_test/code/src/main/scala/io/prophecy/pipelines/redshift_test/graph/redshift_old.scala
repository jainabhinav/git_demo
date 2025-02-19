package io.prophecy.pipelines.redshift_test.graph

import io.prophecy.libs._
import io.prophecy.pipelines.redshift_test.config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object redshift_old {

  def apply(context: Context): DataFrame = {
    import com.databricks.dbutils_v1.DBUtilsHolder.dbutils
    val spark = context.spark
    import spark.implicits._
    var reader = spark.read.format("com.databricks.spark.redshift")
    reader = reader
      .option("forward_spark_s3_credentials", true)
      .option("tempDir",                      "asfsdg")
      .option("url",                          "jdbc:redshift://214asd:11/urlasd")
      .option("user",                         s"${context.config.abc}")
      .option("password",
              s"${dbutils.secrets.get(scope = "asde", key = "password")}"
      )
    reader.load()
  }

}
