package io.prophecy.pipelines.dxf_framework.graph

import io.prophecy.libs._
import io.prophecy.pipelines.dxf_framework.config.Context
import io.prophecy.pipelines.dxf_framework.udfs.UDFs._
import io.prophecy.pipelines.dxf_framework.udfs.PipelineInitCode._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object ht2 {
  def apply(context: Context, in0: DataFrame): DataFrame = {
    val spark = context.spark
    val Config = context.config
    import org.apache.spark.sql.{SparkSession}
    import org.apache.spark.sql.types.StructType
    import org.apache.spark.sql.functions._
    
    println("########################")
    println("#####Step name: ht2#####")
    println("########################")
    println(
          "step start time: " + Instant.now().atZone(ZoneId.of("America/Chicago"))
        ) 
    
    var out0 = in0
    
    if (Config.ht2_flag && (spark.conf.get("new_data_flag") == "true" )) {
      def flattenStructSchema(
          schema: StructType,
          prefix: String = null
      ): Array[Column] = {
        schema.fields.flatMap(f => {
          val columnName = if (prefix == null) f.name else s"$prefix.${f.name}"
    
          f.dataType match {
            case st: StructType => flattenStructSchema(st, columnName)
            case _ => Array(col(columnName).as(columnName.replace(".", "-")))
          }
        })
      }
    
      val flattenedSchemaDf = in0.select(flattenStructSchema(in0.schema): _*)
    
      var exprStringBuilder = ""
      val tempSet = scala.collection.mutable.Set[String]()
    
      flattenedSchemaDf.columns.foreach { x =>
        val childName = x.split('-').reverse.array(0)
        // exprStringBuilder ++= s"`$x`${if(tempSet.contains(childName)) "" else " as " + childName}, "
        if (tempSet.contains(childName) & (x.split('-').size > 1) ) {
          exprStringBuilder =  exprStringBuilder + "`" + x + "`" + ", "
        } else if (tempSet.contains(childName)){
          exprStringBuilder = exprStringBuilder + "`" + x + "`" + " as `outer_struct-" + x + "`" + ", "
        } else {
          exprStringBuilder = exprStringBuilder + "`" + x + "`" + " as " + "`" + childName + "`" + ", "
        }
    
        tempSet += childName
      }
    
      val finalColumnsList = exprStringBuilder.dropRight(2).toString.split(",")
    
      val targetTableColumns =
        spark.read.table(s"${Config.target_table_db}.${Config.target_table.replace("ht2_","")}").columns
    
      val filteredColumnsList = 
        targetTableColumns.diff(Config.audit_cols.split(",").map(_.trim))
    
      out0 = flattenedSchemaDf
        .select(finalColumnsList.map(expr): _*)
        .withColumn("file_name_timestamp", coalesce(col("file_name_timestamp"), lit("19000101000000"))).withColumn("rec_stat_cd", lit("1"))
    }
    out0
  }

}
