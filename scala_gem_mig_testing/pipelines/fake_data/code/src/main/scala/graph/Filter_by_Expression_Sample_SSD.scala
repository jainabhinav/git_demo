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

object Filter_by_Expression_Sample_SSD {

  def apply(context: Context, in: DataFrame): DataFrame =
    in.filter(col("auction_id_64") % lit(100) === lit(17))

}
