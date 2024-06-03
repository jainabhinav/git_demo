package graph.Create_sup_lookup_files

import io.prophecy.libs._
import udfs.PipelineInitCode._
import udfs.UDFs._
import graph.Create_sup_lookup_files.config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Filter_non_IPv4_address_and_IPv4_Business_IP_s {

  def apply(context: Context, in: DataFrame): DataFrame =
    in.filter(
      (col("ip_feature") === lit(3))
        .and(col("name") === lit("home"))
        .or(col("ip_feature") === lit(7))
        .and(
          is_not_null(
            re_get_match(col("start_ip").cast(StringType),
                         lit("^(?:[0-9]{1,3}\\.){3}[0-9]{1,3}")
            )
          )
        )
    )

}
