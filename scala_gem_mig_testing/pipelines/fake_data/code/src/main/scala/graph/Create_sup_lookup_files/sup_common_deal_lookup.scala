package graph.Create_sup_lookup_files

import io.prophecy.libs._
import graph.Create_sup_lookup_files.config.Context
import udfs.UDFs._
import udfs.PipelineInitCode._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object sup_common_deal_lookup {

  def apply(context: Context, in: DataFrame): Unit =
    createLookup("sup_common_deal",
                 in,
                 context.spark,
                 List("id"),
                 "id",
                 "member_id",
                 "deal_type_id"
    )

}
