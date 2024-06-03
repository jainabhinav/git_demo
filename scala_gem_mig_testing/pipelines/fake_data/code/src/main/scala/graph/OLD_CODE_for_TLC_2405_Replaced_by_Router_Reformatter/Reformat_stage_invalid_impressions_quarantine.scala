package graph.OLD_CODE_for_TLC_2405_Replaced_by_Router_Reformatter

import io.prophecy.libs._
import graph.OLD_CODE_for_TLC_2405_Replaced_by_Router_Reformatter.config.Context
import udfs.UDFs._
import udfs.PipelineInitCode._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Reformat_stage_invalid_impressions_quarantine {
  def apply(context: Context, in: DataFrame): DataFrame = {
    val spark = context.spark
    val Config = context.config
    
      import _root_.io.prophecy.abinitio.ScalaFunctions._
    
    // UDF Definitions part of Config.apply_OLD_CODE_for_TLC_2405_Replaced_by_Router_Reformatter__Reformat_stage_invalid_impressions_quarantine_udf_rules can come here
    
      val out = in.getSelectDataFrame(
        Config.apply_OLD_CODE_for_TLC_2405_Replaced_by_Router_Reformatter__Reformat_stage_invalid_impressions_quarantine_rules
      )
    out
  }

}
