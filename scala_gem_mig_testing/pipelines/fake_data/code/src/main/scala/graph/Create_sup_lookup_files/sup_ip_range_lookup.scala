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

object sup_ip_range_lookup {

  def apply(context: Context, in: DataFrame): Unit =
    createRangeLookup("sup_ip_range",
                      in,
                      context.spark,
                      "start_ip_number",
                      "end_ip_number",
                      "index",
                      "start_ip",
                      "end_ip",
                      "name",
                      "ip_feature",
                      "start_ip_number",
                      "end_ip_number"
    )

}
