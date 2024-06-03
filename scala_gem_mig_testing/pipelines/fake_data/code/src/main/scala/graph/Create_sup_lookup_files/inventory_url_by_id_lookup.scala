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

object inventory_url_by_id_lookup {

  def apply(context: Context, in: DataFrame): Unit =
    createLookup("inventory_url_by_id",
                 in,
                 context.spark,
                 List("inventory_url_id"),
                 "inventory_url_id",
                 "inventory_url"
    )

}
