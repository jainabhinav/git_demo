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

object fetch_config_from_metadata {
  def apply(context: Context, in0: DataFrame): DataFrame = {
    val spark = context.spark
    val Config = context.config
    import com.databricks.dbutils_v1.DBUtilsHolder.dbutils
    println("###############################################")
    println("#####Step name: fetch_config_from_metadata#####")
    println("###############################################")
    println(
          "step start time: " + Instant.now().atZone(ZoneId.of("America/Chicago"))
        ) 
    
    if (Config.pk_sk_col_from_metadata_table) {
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
    
      val fs_access_key =
        dbutils.secrets.get(
          scope = Config.synapse_scope,
          key = Config.synapse_fs_access_key
        )
    
      val synapse_fs_access_url =
        dbutils.secrets.get(
          scope = Config.synapse_scope,
          key = Config.synapse_fs_access_url_key
        )
    
      val synapse_temp_dir =
        dbutils.secrets.get(
          scope = Config.synapse_scope,
          key = Config.synapse_temp_dir_key
        )
    
      spark.conf.set(
        synapse_fs_access_url,
        fs_access_key
      )
      val metadata_query =
        s"select A.TableName, NaturalKeys, SurrKey  from ${Config.sql_server_lookup_table_detail} A inner join ${Config.sql_server_sk_metadata_table} B on A.TableDetail_Id = B.TableDetail_Id where LOWER(tableName)=LOWER('${Config.target_table}')"
    
      println("metadata_query: " + metadata_query)
    
      val sk_metadata_df = spark.read
        .format("jdbc")
        .option(
          "url",
          "jdbc:sqlserver://" + metadata_url + ";database=" + metadata_db_name
        )
        .option("user", metadata_user)
        .option("password", metadata_password)
        .option(
          "query",
          metadata_query
        )
        .option("useAzureMSI", "true")
        .option("tempDir", synapse_temp_dir)
        .load()
      val prim_key_columns = sk_metadata_df
        .selectExpr("nvl(NaturalKeys, '-')")
        .first
        .getAs[String](0)
        .replace(";", ",")
        .toLowerCase
        .trim
      val sk_service_col = sk_metadata_df
        .selectExpr("nvl(SurrKey, '-')")
        .first
        .getAs[String](0)
        .toLowerCase
        .trim
    
      if (prim_key_columns != "-") {
        spark.conf.set("primary_key", prim_key_columns)
      }
      if (sk_service_col != "-") {
        spark.conf.set("sk_service_col", sk_service_col)
      }
      println("prim_key_columns: "+prim_key_columns)
      println("prim_key_columns from json: " + Config.primary_key)
    
      println("sk_service_col: "+sk_service_col)
      println("sk_service_col from json: " + Config.sk_service_col)
    }
    
    val out0 = spark.createDataFrame(Seq(("1", "1"), ("2", "2")))
    out0
  }

}
