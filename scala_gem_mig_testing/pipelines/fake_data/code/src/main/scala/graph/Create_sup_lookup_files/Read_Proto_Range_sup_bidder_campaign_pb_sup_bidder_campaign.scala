package graph.Create_sup_lookup_files

import io.prophecy.libs._
import graph.Create_sup_lookup_files.config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Read_Proto_Range_sup_bidder_campaign_pb_sup_bidder_campaign {

  def apply(context: Context): DataFrame =
    context.spark.read
      .format("csv")
      .option("header", true)
      .option("sep",    ",")
      .load("")

}