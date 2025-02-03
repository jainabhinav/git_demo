package io.prophecy.pipelines.test_pipeline.graph

import io.prophecy.libs._
import io.prophecy.pipelines.test_pipeline.config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object csv {

  def apply(context: Context): DataFrame = {
    val spark = context.spark
    locally {
      import org.apache.spark.sql.Row
      val linesRDD        = spark.sparkContext.textFile("asd")
      val totalCount      = linesRDD.count()
      var skipFooterLines = 0
      if (totalCount > 0) skipFooterLines = (totalCount - 1 - 4).toInt
      val csvRDD = linesRDD.zipWithIndex.filter({
        case (line, idx) =>
          idx >= 2 & idx <= skipFooterLines
      })
      locally {
        val dfSchema = StructType(
          csvRDD
            .take(1)
            .map({
              case (line, idx) =>
                line.split(",")
            })
            .map(arr => Row.fromSeq(arr.toList))
            .take(1)(0)
            .toSeq
            .map(x => StructField(x.toString, StringType))
        )
        val finalRDD = csvRDD
          .filter({
            case (line, idx) =>
              idx >= 2 + 1
          })
          .map({
            case (line, idx) =>
              line.split(",")
          })
          .map(arr => Row.fromSeq(arr.toList))
        locally {
          val df         = spark.createDataFrame(finalRDD, dfSchema)
          var userSchema = None.get.toArray
          df.selectExpr(df.columns.zipWithIndex.map({
            case (c, idx) =>
              s"CAST(`$c` AS ${userSchema(idx).dataType.sql}) as `${userSchema(idx).name}`"
          }): _*)
        }
      }
    }
  }

}
