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

object Filter_by_Expression_has_curator {
  def apply(context: Context, in: DataFrame): DataFrame = {
    val spark = context.spark
    val Config = context.config
    
      val (out0, out1) = in.getFilterDataFrame(
        Config.oLD_CODE_for_TLC_2405_Replaced_by_Router_Reformatter__Filter_by_Expression_has_curator_rules
      )
    out0
  }

}
