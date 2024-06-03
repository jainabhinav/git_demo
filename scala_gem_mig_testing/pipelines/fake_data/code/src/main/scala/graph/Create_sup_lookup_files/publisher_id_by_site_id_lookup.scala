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

object publisher_id_by_site_id_lookup {

  def apply(context: Context, in: DataFrame): Unit =
    createLookup("publisher_id_by_site_id",
                 in,
                 context.spark,
                 List("site_id"),
                 "seller_member_id",
                 "publisher_id",
                 "site_id",
                 "tag_id"
    )

}
