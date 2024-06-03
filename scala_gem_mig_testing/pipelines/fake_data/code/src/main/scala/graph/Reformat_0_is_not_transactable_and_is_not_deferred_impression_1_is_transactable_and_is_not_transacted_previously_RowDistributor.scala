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

object Reformat_0_is_not_transactable_and_is_not_deferred_impression_1_is_transactable_and_is_not_transacted_previously_RowDistributor {

  def apply(context: Context, in: DataFrame): (DataFrame, DataFrame) =
    (in.filter(
       (coalesce(col("is_transactable").cast(ByteType), lit(0)) === lit(0)).and(
         coalesce(col("is_deferred_impression").cast(ByteType), lit(0)) === lit(
           0
         )
       )
     ),
     in.filter(
       !(coalesce(col("is_transactable").cast(ByteType), lit(0)) === lit(0))
         .and(
           coalesce(col("is_deferred_impression").cast(ByteType),
                    lit(0)
           ) === lit(0)
         )
         .and(
           (coalesce(col("is_transactable").cast(ByteType), lit(0)) === lit(1))
             .and(
               coalesce(col("is_transacted_previously").cast(ByteType),
                        lit(0)
               ) === lit(0)
             )
         )
     )
    )

}
