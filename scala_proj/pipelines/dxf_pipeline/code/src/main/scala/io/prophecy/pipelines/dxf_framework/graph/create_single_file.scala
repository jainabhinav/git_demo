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

object create_single_file {
  def apply(context: Context, in0: DataFrame, in1: DataFrame): DataFrame = {
    val spark = context.spark
    val Config = context.config
    import org.apache.hadoop.conf.Configuration
    import org.apache.hadoop.fs.{FSDataInputStream, FileSystem, Path}
    import org.apache.hadoop.io.IOUtils
    import scala.io.Source
    import scala.sys.process.Process
    import scala.util.Try
    import java.io._
    import java.text.SimpleDateFormat
    //read parquets in  folder into single dataframe and write back as single parquet
    println("#######################################")
    println("#####Step name: create_single_file#####")
    println("#######################################")
    println(
      "step start time: " + Instant.now().atZone(ZoneId.of("America/Chicago"))
    )
    
    if (Config.skip_main_table_load_ready_files == "false") {
      if (
        Config.generate_load_ready_files == "true" || (Config.generate_only_load_ready_files == "true")
      ) {
        try {
          println("Renaming and moving insert and update load ready file")
          val load_ready_insert_path =
            Config.load_ready_insert_path
          val baseFilePath =
            Config.single_load_ready_path
          val tableName = Config.target_table
          val run_id = spark.conf.get("run_id")
    
          val hadoopConfig = new Configuration()
          val hdfs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    
          val finalInsertPath = new Path(
            baseFilePath + "/" + tableName + "/" + run_id.substring(
              0,
              4
            ) + "/" + tableName + ".insert.000." + run_id + ".parquet"
          )
          val finalUpdatePath = new Path(
            baseFilePath + "/" + tableName + "/" + run_id.substring(
              0,
              4
            ) + "/" + tableName + ".update.000." + run_id + ".parquet"
          )
    
          val tempInsertPath =
            new Path(
              Config.load_ready_insert_path + "/" + Config.target_table + "/insert_files/" + spark.conf
                .get("run_id") + "/"
            )
          val tempUpdatePath =
            new Path(
              Config.load_ready_update_path + "/" + Config.target_table + "/update_files/" + spark.conf
                .get("run_id") + "/"
            )
    
          copyMerge(hdfs, tempInsertPath, hdfs, finalInsertPath, true, hadoopConfig)
          copyMerge(hdfs, tempUpdatePath, hdfs, finalUpdatePath, true, hadoopConfig)
    
          def copyMerge(
              srcFS: FileSystem,
              srcDir: Path,
              dstFS: FileSystem,
              dstFile: Path,
              deleteSource: Boolean,
              conf: Configuration
          ): Boolean = {
            if (dstFS.exists(dstFile))
              throw new IOException(s"Target $dstFile already exists")
    
            // Source path is expected to be a directory:
            if (srcFS.getFileStatus(srcDir).isDirectory()) {
    
              val outputFile = dstFS.create(dstFile)
              Try {
                srcFS
                  .listStatus(srcDir)
                  .sortBy(_.getPath.getName)
                  .filter(_.getPath.getName.endsWith(".parquet"))
                  .collect {
                    case status if status.isFile() =>
                      val inputFile = srcFS.open(status.getPath())
                      Try(IOUtils.copyBytes(inputFile, outputFile, conf, false))
                      inputFile.close()
                  }
              }
              outputFile.close()
    
              if (deleteSource) srcFS.delete(srcDir, true) else true
            } else false
          }
        } catch {
          case e: Exception => {
            if (spark.conf.get("main_table_api_type") == "NON-API") {
              val update_sql = spark.conf.get("main_table_max_sk_update_sql")
              println(
                s"""Updating max sk in metadata table failed. Please update max sk manually and remove lock to proceed.
                Update metadata table run this query in metadata db: ${update_sql}
                To remove lock for ${Config.target_table}. Run below command in databricks: 
                dbutils.fs.rm(${Config.sk_lock_file_path}${Config.sk_table_name_override}.txt")
                """
              )
            }
            // dbutils.fs.rm(Config.sk_lock_file_path + Config.target_table + ".txt")
            throw e
          }
        }
      }
    }
    val out0 = in0
    out0
  }

}
