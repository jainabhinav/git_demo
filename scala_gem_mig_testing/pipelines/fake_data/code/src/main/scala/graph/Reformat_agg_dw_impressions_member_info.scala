package graph

import io.prophecy.libs._
import udfs.PipelineInitCode._
import udfs.UDFs._
import config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Reformat_agg_dw_impressions_member_info {

  def apply(context: Context, in: DataFrame): DataFrame =
    in.select(
      when(
        (coalesce(col("imp_type").cast(IntegerType), lit(1)) === lit(1))
          .or(coalesce(col("imp_type").cast(IntegerType), lit(1)) === lit(2))
          .or(coalesce(col("imp_type").cast(IntegerType), lit(1)) === lit(3))
          .or(coalesce(col("imp_type").cast(IntegerType), lit(1)) === lit(4))
          .or(coalesce(col("imp_type").cast(IntegerType), lit(1)) === lit(6))
          .or(coalesce(col("imp_type").cast(IntegerType), lit(1)) === lit(8)),
        col("seller_member_id").cast(IntegerType)
      ).otherwise(col("buyer_member_id").cast(IntegerType))
        .cast(IntegerType)
        .as("member_id"),
      lit(1).cast(LongType).as("member_count")
    )

}
