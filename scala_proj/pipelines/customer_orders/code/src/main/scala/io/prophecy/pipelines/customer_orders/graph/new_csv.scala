package io.prophecy.pipelines.customer_orders.graph

import io.prophecy.libs._
import io.prophecy.pipelines.customer_orders.config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object new_csv {

  def apply(context: Context): DataFrame =
    context.spark.read
      .format("csv")
      .option("header",                         true)
      .option("sep",                            ",")
      .schema(StructType(Array(StructField("a", StringType, true))))
      .load("asd")

}
