package io.prophecy.pipelines.dxf_framework

import io.prophecy.libs._
import io.prophecy.pipelines.dxf_framework.config._
import io.prophecy.pipelines.dxf_framework.udfs.UDFs._
import io.prophecy.pipelines.dxf_framework.udfs.PipelineInitCode._
import io.prophecy.pipelines.dxf_framework.graph._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Main {

  def apply(context: Context): Unit = {
    val df_spark_configs    = spark_configs(context)
    val df_generate_lookups = generate_lookups(context, df_spark_configs)
    val df_fetch_config_from_metadata =
      fetch_config_from_metadata(context, df_generate_lookups)
    val df_read_source = read_source(context, df_fetch_config_from_metadata)
    val df_ht2         = ht2(context,         df_read_source)
    val df_cleanse_hex = cleanse_hex(context, df_ht2)
    val df_optional_filter_and_audit_fields =
      optional_filter_and_audit_fields(context, df_cleanse_hex)
    val df_optional_repartition =
      optional_repartition(context, df_optional_filter_and_audit_fields)
    val df_put_src_env_sk = put_src_env_sk(context, df_optional_repartition)
    val df_apply_source_default =
      apply_source_default(context, df_put_src_env_sk)
    val df_optional_joins = optional_joins(context, df_apply_source_default)
    val df_optional_filter_rules =
      optional_filter_rules(context, df_optional_joins)
    val df_optional_normalize =
      optional_normalize(context, df_optional_filter_rules)
    val df_optional_order_by = optional_order_by(context, df_optional_normalize)
    val df_optional_default_rules =
      optional_default_rules(context, df_optional_order_by)
    val df_apply_reformat_rules =
      apply_reformat_rules(context, df_optional_default_rules)
    val df_create_ht2_encrypted_file =
      create_ht2_encrypted_file(context, df_apply_reformat_rules)
    val df_self_join_sk      = self_join_sk(context,      df_create_ht2_encrypted_file)
    val df_survivorship_rule = survivorship_rule(context, df_self_join_sk)
    val df_dedup_filter      = dedup_filter(context,      df_survivorship_rule)
    val df_apply_dedup_rules = apply_dedup_rules(context, df_dedup_filter)
    val (df_apply_len_rules_out0, df_apply_len_rules_out1) = {
      val (df_apply_len_rules_out0_temp, df_apply_len_rules_out1_temp) =
        apply_len_rules(context, df_apply_dedup_rules)
      (df_apply_len_rules_out0_temp.cache(),
       df_apply_len_rules_out1_temp.cache()
      )
    }
    reject_records(context, df_apply_len_rules_out1)
    val df_optional_rollup = optional_rollup(context, df_apply_len_rules_out0)
    val df_apply_default_rules =
      apply_default_rules(context, df_optional_rollup)
    val df_sk_service_placeholder =
      sk_service_placeholder(context, df_apply_default_rules)
    val df_add_audit_cols = add_audit_cols(context, df_sk_service_placeholder)
    val df_optional_post_filter =
      optional_post_filter(context, df_add_audit_cols)
    val df_select_final_cols =
      select_final_cols(context, df_optional_post_filter)
    val df_encryption        = encryption(context,        df_select_final_cols)
    val df_write_target      = write_target(context,      df_encryption)
    val df_create_delta_file = create_delta_file(context, df_write_target)
    val df_decryption        = decryption(context,        df_create_delta_file)
    val (df_split_load_ready_files_out0, df_split_load_ready_files_out1) =
      split_load_ready_files(context, df_decryption)
    val df_create_insert_load_ready_file =
      create_insert_load_ready_file(context, df_split_load_ready_files_out0)
    val df_create_update_load_ready_file =
      create_update_load_ready_file(context, df_split_load_ready_files_out1)
    val df_create_single_file = create_single_file(
      context,
      df_create_insert_load_ready_file,
      df_create_update_load_ready_file
    )
    val df_create_trigger_file =
      create_trigger_file(context, df_create_single_file)
    val df_update_max_sk = update_max_sk(context, df_create_trigger_file)
    update_inc_metadata_table(context, df_update_max_sk)
    write_audit_log(context,           df_update_max_sk)
  }

  def main(args: Array[String]): Unit = {
    val config = ConfigurationFactoryImpl.getConfig(args)
    val spark: SparkSession = SparkSession
      .builder()
      .appName("Prophecy Pipeline")
      .config("spark.default.parallelism",             "4")
      .config("spark.sql.legacy.allowUntypedScalaUDF", "true")
      .enableHiveSupport()
      .getOrCreate()
    val context = Context(spark, config)
    spark.conf.set("prophecy.metadata.pipeline.uri",        "pipelines/dxf_pipeline")
    spark.conf.set("spark.sql.legacy.timeParserPolicy",     "LEGACY")
    spark.conf.set("spark.sql.legacy.allowUntypedScalaUDF", "true")
    registerUDFs(spark)
    MetricsCollector.instrument(spark, "pipelines/dxf_pipeline") {
      apply(context)
    }
  }

}
