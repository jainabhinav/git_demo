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

object sup_bidder_campaign_lookup {

  def apply(context: Context, in: DataFrame): Unit =
    createLookup("sup_bidder_campaign",
                 in,
                 context.spark,
                 List("campaign_id"),
                 "campaign_id",
                 "campaign_type_id",
                 "campaign_group_id",
                 "campaign_group_type_id"
    )

}
