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

object update_max_sk {
  def apply(context: Context, in0: DataFrame): DataFrame = {
    val spark = context.spark
    val Config = context.config
    println("#################################################")
    println("#####Step name: update max_sk for main table#####")
    println("#################################################")
    println(
      "step start time: " + Instant.now().atZone(ZoneId.of("America/Chicago"))
    )
    
    if (
      Config.enable_sk_service != "false" && spark.conf.get(
        "new_data_flag"
      ) == "true"
    ) {
      import com.databricks.dbutils_v1.DBUtilsHolder.dbutils
      val new_max_sk = spark.conf.get("main_table_max_sk").toLong
      val new_sk_count = spark.conf.get("main_table_new_sk_count").toLong
      val sk_table_id = spark.conf.get("main_table_table_id").toInt
      if (spark.conf.get("main_table_api_type") == "NON-API" && new_sk_count > 0) {
    
        import java.io.{IOException, _}
        import java.time._
        import java.time.format._
        import java.util
        import java.sql.DriverManager
        import java.util.Properties
        import scala.language.implicitConversions
        import scala.util.Try
    
        val metadata_db_name =
        dbutils.secrets.get(
          scope = Config.metadata_scope,
          key = Config.metadata_dbname_key
        )
    
        val metadata_url =
        dbutils.secrets.get(
          scope = Config.metadata_scope,
          key = Config.metadata_url_key
        )
    
        val metadata_user =
        dbutils.secrets.get(
          scope = Config.metadata_scope,
          key = Config.metadata_user_key
        )
    
        val metadata_password =
        dbutils.secrets.get(
          scope = Config.metadata_scope,
          key = Config.metadata_password_key
        )
    
        // update max_sk
        // Create the JDBC URL without passing in the user and password parameters.
        val jdbcUrl =
          s"jdbc:sqlserver://${metadata_url}:1433;database=${metadata_db_name}"
    
        // Create a Properties() object to hold the parameters.
        val connectionProperties = new Properties()
        connectionProperties.put("user", s"${metadata_user}")
        connectionProperties.put("password", s"${metadata_password}")
    
        val driverClass = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
        connectionProperties.setProperty("Driver", driverClass)
    
        val con =
          DriverManager.getConnection(jdbcUrl, connectionProperties)
        val stmt = con.createStatement()
    
        val update_sql = spark.conf.get("main_table_max_sk_update_sql")
        println("dummy update_sql: " + update_sql)
        try {
          stmt.execute(update_sql)
          println(
            "update max sk successful: " + Config.sk_table_name_override + " with value: " + new_max_sk
          )
        } catch {
          case e: Exception => {
            println(
              s"""Updating max sk in metadata table failed. Please update max sk manually and remove lock to proceed.
              To update metadata table run this query in metadata db: ${update_sql}
              To remove lock for ${Config.target_table}. Run below command in databricks: 
              dbutils.fs.rm(${Config.sk_lock_file_path}${Config.sk_table_name_override}.txt")
              """
            )
    
            // dbutils.fs.rm(Config.sk_lock_file_path + Config.target_table + ".txt")
            throw e
          }
        }
        
    
      }
      if (spark.conf.get("main_table_api_type") == "NON-API"){
        println("Removing lock from main table: " + Config.target_table)
        dbutils.fs.rm(
          Config.sk_lock_file_path + Config.sk_table_name_override + ".txt"
        )
      }
    
    }
    val out0 = in0
    out0
  }

}
