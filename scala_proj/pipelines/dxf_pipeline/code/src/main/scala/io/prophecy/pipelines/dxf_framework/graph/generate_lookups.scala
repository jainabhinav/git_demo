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

object generate_lookups {
  def apply(context: Context, in0: DataFrame): DataFrame = {
    val spark = context.spark
    val Config = context.config
    println("#####################################")
    println("#####Step name: generate_lookups#####")
    println("#####################################")
    println(
          "step start time: " + Instant.now().atZone(ZoneId.of("America/Chicago"))
        ) 
    
    // case class to define lookup definition
    case class LookupDef(
        fmt: String,
        src: String,
        keyCols: List[String],
        valCols: List[String]
    )
    
    type Lookups = Map[String, LookupDef]
    
    // create lookups if lookup is not empty
    if (Config.lookups != "None" || (Config.survivorship_lookup != "None" && Config.survivorship_flag)) {
      import pureconfig._
      import pureconfig.generic.auto._
    
      val lookupConfig = if (Config.lookups != "None") {
        ConfigSource.string(Config.lookups).loadOrThrow[Lookups]
      } else {
        Map.empty[String, LookupDef]
      }
    
      val survivorshipLookupConfig = if (Config.survivorship_lookup != "None") {
        ConfigSource.string(Config.survivorship_lookup).loadOrThrow[Lookups]
      } else {
        Map.empty[String, LookupDef]
      }
    
      val final_lookup_config = lookupConfig.++(survivorshipLookupConfig)
    
      // iterate through list of lookups based on different source types and create lookup
      final_lookup_config.foreach({
        case (lookupName, lookupDef) => {
          val in0 = if (lookupDef.fmt == "csv") {
            spark.read
              .format("csv")
              .option("header", true)
              .option("sep", ",")
              .load(lookupDef.src)
          } else if (lookupDef.fmt == "parquet") {
            spark.read
              .format("parquet")
              .load(lookupDef.src)
          } else if (lookupDef.fmt == "query") {
            spark.sql(lookupDef.src)
          } else {
            spark.read.table(lookupDef.src)
          }
    
          var in1 = in0
            .select(in0.columns.map(x => col(x).as(x.toLowerCase)): _*)
            .select((lookupDef.keyCols ++ lookupDef.valCols).map(x => col(x)): _*)
          
          createLookup(
            lookupName,
            in1,
            spark,
            lookupDef.keyCols,
            lookupDef.valCols: _*
          )
        }
      })
    
    }
    
    val out0 = spark.createDataFrame(Seq(("1", "1"), ("2", "2")))
    out0
  }

}
