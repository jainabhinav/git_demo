package io.prophecy.pipelines.mongo_db.graph

import io.prophecy.libs._
import io.prophecy.pipelines.mongo_db.config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object mongodb_old_config_1 {

  def apply(context: Context, df: DataFrame): Unit =
    df.write
      .format("mongodb")
      .mode("overwrite")
      .option("connection.uri", "cvb://:@ewrn".trim)
      .option("database",       "cnjmy")
      .option("collection",     "cvnb")
      .save()

}
