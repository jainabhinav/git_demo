package io.prophecy.pipelines.dxf_framework.config

import pureconfig._
import pureconfig.generic.ProductHint
import io.prophecy.libs._

case class Config(
  @Description(
    "Name of the pipeline. This is used for auditing, event and consumption logs."
  ) var pipeline_name: String = "pipeline_name",
  @Description(
    "Source table name. Used for auditing purpose."
  ) var source_table: String = "source_table",
  @Description(
    "Database name for target delta table"
  ) var target_table_db: String = "orx_idw_dm_prd",
  @Description("Name of target delta table") var target_table: String =
    "target_table",
  @Description(
    "Path from where source data needs to be read"
  ) var source_path: String = """srcs = [{
    "fmt" : "parquet",
    "src" : "dbfs:/path_to_file"
}]""",
  @Description(
    "Flag to identify if only incremental files need to be read from directory"
  ) var read_incremental_files_flag: String = "true",
  @Description(
    "Json specifying target column name and expression"
  ) var reformat_rules: String = "None",
  @Description(
    "Config to define join conditions on base tables"
  ) var reformat_rules_join: String = "joins = []",
  @Description(
    "Option config to define lookups. Lookups in spark remain are broadcasted in memory. Avoid using it for very large tables."
  ) var lookups: String = "None",
  @Description(
    "Comma separated list of natural key of table"
  ) var primary_key: String = "pk",
  @Description(
    "Length rules if any to reject the record"
  ) var length_rules: String = "None",
  @Description(
    "Optional filter condition to filter out the data at source"
  ) var source_filter: String = "None",
  @Description(
    "Setting true would make the output as temp output and would overwrite the table instead of CDC"
  ) var temp_output_flag: String = "false",
  @Description(
    "Option setting to provide deduplication rules, by default deduplication would be done on primary key if no rules i sgiven"
  ) var dedup_rules: String = "None",
  @Description(
    "final schema of table as per delta table"
  ) var final_table_schema: String = "None",
  @Description(
    "Comma separated columns if any which needs to be removed from duplicate row check"
  ) var hash_cols:                                           String = "None",
  @Description("SK column of the table") var sk_service_col: String = "None",
  @Description(
    "FSK config of the table which needs to be configured for making the call to SK service"
  ) var optional_sk_config: String = "sks = []",
  @Description(
    "Path where load ready insert files are created"
  ) var load_ready_insert_path: String = "dbfs:/mnt/prophecy_dev/load_ready",
  @Description(
    "Path where load ready update files are created"
  ) var load_ready_update_path: String = "dbfs:/mnt/prophecy_dev/load_ready",
  @Description(
    "Path where single load ready files are stored"
  ) var single_load_ready_path: String =
    "dbfs:/mnt/prophecy_dev/single_load_ready",
  @Description(
    "Group by columns for Produce block in abinitio"
  ) var rollup_groupby_rules: String = "None",
  @Description(
    "Aggregate rules for Produce block in abinitio"
  ) var rollup_agg_rules: String = "None",
  @Description(
    "Rows to filter if any just before writing to target"
  ) var target_filter: String = "None",
  @Description("Comma separated order by rules") var orderby_rules: String =
    "None",
  @Description(
    "Column which needs to be normalised"
  ) var normalize_rules: String = "None",
  @Description(
    "Threshold value of reject records in a run beyond which pipeline should fail. Default is 0."
  ) var reject_record_threshold: Int = 0,
  @Description(
    "If true data at source would be reparitioned"
  ) var repartition_flag: String = "false",
  @Description("Column to repartition on") var repartition_cols: String =
    "None",
  @Description("Columns to decrypt if any") var decrypt_cols: String = "None",
  @Description(
    "Flag to enable/disable SK service api calls"
  ) var enable_sk_service: String = "false",
  @Description(
    "Flag to enable self join on table to reduce the number of requests to SK service API "
  ) var sk_service_self_join: String = "true",
  @Description(
    "Flag to enable/disable load ready files"
  ) var generate_load_ready_files: String = "true",
  @Description(
    "Flag to enable/disable creation of placeholder records"
  ) var create_placeholders: String = "false",
  @Description(
    "Watermark table where last processed timestamp for each pipeline would be stored"
  ) var incremental_load_metadata_table: String =
    "temp.incremental_load_metadata_table",
  @Description(
    "Table name where audit summary needs to be stored"
  ) var audit_summary: String = "temp.audit_summary",
  @Description(
    "If true then audit columns won't be added to data"
  ) var disable_audit: String = "false",
  @Description(
    "If true then reject record summary won't be populated"
  ) var disable_reject_record_summary: String = "true",
  @Description(
    "If true then audit summary won't be populated"
  ) var disable_audit_summary: String = "false",
  @Description(
    "if false then decimal type columns wold not be converted to int/long at the source based on data"
  ) var default_decimal_types: String = "true",
  @Description(
    "default value for rec_stat_cd. Set to 1."
  ) var rec_stat_cd: String = "1",
  @Description("Constant unique identifer for datamart") var uid: String =
    "None",
  @Description(
    "Audit columns list to be excluded from duplicate row check while performing CDC"
  ) var audit_cols: String =
    "insert_ts, update_ts, insert_uid, update_uid, run_id, rec_stat_cd",
  @Description(
    "Advanced default rules in key value json"
  ) var default_rules: String = "None",
  @Description(
    "Table in which error summary would be stored"
  ) var error_summary_table: String = "temp.hd_error_summary",
  @Description(
    "Prefix for table in which rejected records would be saved"
  ) var error_table_prefix:                                   String = "temp.error_",
  @Description("default value of src_env_sk") var src_env_sk: String = "690",
  @Description(
    "Parallel calls for SK service at a time"
  ) var sk_service_parallel_pool: Int = 5,
  @Description(
    "Placeholder config containing mapping of all table for the data mart"
  ) var pk_sk_info: String = "{}",
  @Description(
    "if true, deduplication rules would be disabled"
  ) var disable_dedup_rules: String = "false",
  @Description(
    "Number of key's to be sent to SK service in one api call"
  ) var sk_service_batch_size: Int = 30000,
  @Description(
    "If false default rules as per data types won't be applied"
  ) var apply_defaults: String = "true",
  @Description("Base URL for sk service") var sk_service_base_url: String =
    "None",
  @Description(
    "Max number of rows to stream from SK service api for placeholder records creation"
  ) var sk_stream_max_rows: Int = 1000,
  @Description(
    "List of columns which needs to be defaulted before reformat rules are applied"
  ) var default_source_blank_nulls: String = "None",
  @Description(
    "Parallel threads to be spawned for get stream calls to SK service used to create placeholder records"
  ) var sk_stream_parallel_pool: Int = 1,
  @Description(
    "read input data from multiple tables"
  ) var read_from_multiple_temp_tables: String = "false",
  @Description(
    "Strategy to write into target. SCD0, SCD1 etc"
  ) var target_write_type: String = "scd1",
  @Description(
    "When set to true, join's are persisted in disk after each stage. Make it true when for large tables."
  ) var join_persist_flag: String = "false",
  @Description(
    "Skip writing to delta table and synapse for common dimensions"
  ) var skip_delta_synapse_write_common_dimension: String = "false",
  @Description(
    "List of tables for which delta and synapse write needs to be skipped"
  ) var list_tables_skip_delta_synapse_write_common_dimension_placeholder: String =
    "None",
  @Description("Data mart name") var data_mart: String = "home_delivery",
  @Description(
    "Table name where events would be stored for synapse consumption"
  ) var event_log_table_name: String = "temp.event_log",
  @Description("Decryption secret scope") var decrypt_scope: String =
    "idwshared-dbscope",
  @Description("Decryption secret key") var decrypt_EncKey: String = "EncKey",
  @Description("Decryption secret init vector") var decrypt_InitVec: String =
    "InitVec",
  @Description(
    "Databricks secrets scope for SK service api key"
  ) var api_scope: String = "idwshared-dbscope",
  @Description(
    "Databricks secrets key for SK service api key"
  ) var api_key: String = "databricks-user-key",
  @Description(
    "In case source data has duplicates on primary key, you can give comma separated list on which dedup should happen"
  ) var dedup_columns: String = "None",
  @Description("Join hint for source dataframe") var source_join_hint: String =
    "None",
  @Description("Custom run_id HHmmss suffix") var custom_run_id_suffix: String =
    "None",
  @Description(
    "if true it will add sec_flg audit column to final output"
  ) var add_audit_sec_flag: String = "false",
  @Description(
    "If only load ready files needs to be generated, make this flag true"
  ) var generate_only_load_ready_files:                       String = "false",
  @Description("Columns to encrypt if any") var encrypt_cols: String = "None",
  @Description("Encryption secret scope") var encrypt_scope: String =
    "idwshared-dbscope",
  @Description("Encryption secret key") var encrypt_EncKey: String = "EncKey",
  @Description("Encryption secret init vector") var encrypt_InitVec: String =
    "InitVec",
  @Description(
    "If true length rules would be taken from metadata table"
  ) var length_rules_from_metadata_table: String = "false",
  @Description(
    "Metadata table name for schema and length rules"
  ) var length_rules_metadata_table: String = "temp.synapse_schema_metadata",
  @Description(
    "Run a SQL query before generation of load ready files"
  ) var post_delta_sql_update: String = "None",
  @Description(
    "Prefix to add in trigger file content. For e.g for home delivery it's hd"
  ) var trigger_file_content_data_mart_prefix: String = "hd",
  @Description(
    "Filter to apply to data before deduplication"
  ) var dedup_filter: String = "None",
  @Description(
    "Populate run_id column in data with timestamp in file name "
  ) var run_id_from_filename: String = "false",
  @Description(
    "Databricks secrets scope for ff3 encrypt"
  ) var ff3_encrypt_scope: String = "idwshared-dbscope",
  @Description(
    "Databricks secrets key for ff3 encrypt key"
  ) var ff3_encrypt_key: String = "ff3EncKey",
  @Description(
    "Databricks secrets key for ff3 encrypt tweak"
  ) var ff3_encrypt_tweak: String = "ff3InitVec",
  @Description(
    "Optional filter before reformat rules"
  ) var optional_filter_rules: String = "None",
  @Description(
    "Join if any required for optional filter"
  ) var optional_filter_rules_join: String = "None",
  @Description(
    "Hex characters to clean in source"
  ) var hex_cleanse_pattern: String =
    "[\\x01\\x02\\x03\\x04\\x05\\x06\\x07\\x08\\x09\\x0a\\x0b\\x0c\\x0d\\x0e\\x0f\\x10\\x11\\x12\\x13\\x14\\x15\\x16\\x17\\x18\\x19\\x1a\\x1b\\x1c\\x1d\\x1e\\x1f\\xc7\\xff\\xa0\\xa4\\xa6\\xa8\\xb4\\xb8\\xbc\\xbd\\xbe]",
  @Description(
    "Value to replace hex characters with"
  ) var hex_replace_value: String = "  ",
  @Description(
    "End time strategy to pick incremental data. Currently supported: now, today and today-n (where n is number of days).)"
  ) var incremental_table_strategy: String = "now",
  @Description(
    "Delta hours to add to end time strategy to pick incremental data. e.g.: for 6:30 AM we can give 06:30:00"
  ) var incremental_table_strategy_delta_hours: String = "None",
  @Description(
    "Column on which incremental data needs to be picked up. Can be spark sql expression as well"
  ) var incremental_table_watermark_col: String = "UPDATE_TIMESTAMP",
  @Description(
    "Cutoff date for table when entry not present in incremental metadata table. During initial load what data to consider"
  ) var incremental_table_cutoff_date: String = "1900-01-01",
  @Description(
    "Incremental comparison strategy for columns (incremental_col lower_cond last_load_timestamp and incremental_col upper_cond strategy_timestamp). Possible values for lower_cond, upper_cond can be  >, < or >=,<= or >=,< or >,<="
  ) var incremental_condition_strategy: String = ">,<=",
  @Description(
    "enable/disable survivorship rules"
  ) var survivorship_flag: Boolean = true,
  @Description(
    "comma separated list of exception tables for survivorship rule"
  ) var survivorship_override_tables: String =
    "d_pharmacy_affiliation, d_pharmacy_payment_center, d_pharmacy, d_pharmacy_relationship, d_pharmacy_rltnshp_dtl, d_pharmacy_network, d_pharmacy_network_rcex1p, d_pharmacy_network_rcphdp, d_pharmacy_super_network, d_pharmacy_super_network_rcex1p, d_product",
  @Description("Lookup for survivorship rule") var survivorship_lookup: String =
    """{
    "lookup_src_envrt_id" : {
        "fmt" : "parquet",
        "key-cols" : [
            "src_env_sk",
            "tgt_tbl_nm"
        ],
        "src" : "dbfs:/mnt/shared/survivorship_lookup/lookup_src_envrt_id.parquet",
        "val-cols" : [
            "src_env_rnk"
        ]
    }
}""",
  @Description(
    "spark configs to set at start of the pipeline"
  ) var spark_configs: String =
    "spark.sql.shuffle.partitions=800,spark.sql.legacy.allowUntypedScalaUDF=true,spark.sql.legacy.timeParserPolicy=LEGACY",
  @Description(
    "comma separated list of partition cols. eg: col1, col2"
  ) var remove_partition_cols_in_load_ready: String = "None",
  @Description(
    "start timestamp placeholder within source query"
  ) var start_ts_query_repr: String = "##START_TS##",
  @Description(
    "end timestamp placeholder within source query"
  ) var end_ts_query_repr: String = "##END_TS##",
  @Description(
    "flag for enabling/disabling incremental source query representation(start_ts_query_repr and end_ts_query_repr)"
  ) var incremental_query_replace_strategy: String = "None",
  @Description(
    "flag to decide to enable new NON-API sk logic "
  ) var sk_service_logic_from_metadata_table: String = "false",
  @Description(
    "metadata table which defines whether to use old or new SK logic per table"
  ) var sql_server_sk_metadata_table: String = "metadata.TableProperties",
  @Description(
    "lookup table for tableId"
  ) var sql_server_lookup_table_detail: String = "metadata.TableDetail",
  @Description(
    "table in which max sk for new non-api SK service would be stored"
  ) var sql_server_max_sk_table: String = "metadata.TableMaxSk",
  @Description(
    "file path at which locks would be created for new non-api sk logic"
  ) var sk_lock_file_path: String = "dbfs:/prophecy/non_api_sk_temp_locks/",
  @Description(
    "Number of times to retry new non-api sk lock"
  ) var sk_lock_retry_count:                                String = "50",
  var synapse_scope:                                        String = "idwshared-dbscope",
  var synapse_user_key:                                     String = "dwUserName",
  var synapse_password_key:                                 String = "dwPassword",
  var synapse_url_key:                                      String = "dwServerURL",
  var synapse_db_name_key:                                  String = "dwDatabaseName",
  var synapse_fs_access_key:                                String = "SAAccessKey",
  var synapse_fs_access_url_key:                            String = "dwStorageAccessURL",
  var synapse_temp_dir_key:                                 String = "dwPolybaseWorkDirectory",
  var max_sk_counter_reset_for_main_table:                  String = "false",
  @Description("flag for enabling HT2 logic") var ht2_flag: Boolean = false,
  var scd2_columns: String =
    "startTimestamp : column_from_dt, endTimestamp : column_thru_dt",
  var pk_sk_col_from_metadata_table: Boolean = false,
  var encryption_to_uppercase:       Boolean = false,
  @Description(
    "merge logic includes sk column of table"
  ) var merge_logic_including_sk:        Boolean = false,
  var metadata_scope:                    String = "idwshared-dbscope",
  var metadata_dbname_key:               String = "sqlDatabase",
  var metadata_url_key:                  String = "dwServerURL",
  var metadata_user_key:                 String = "dwUserName",
  var metadata_password_key:             String = "dwPassword",
  var source_zoneId:                     String = "America/Chicago",
  var incremental_watermark_max_ts_expr: String = "None",
  var scd2_exclude_source_list:          String = "None",
  @Description(
    "skip delta write and load ready file generation for all FSK tables"
  ) var skip_placeholder_tables_delta_load_ready: String = "false",
  @Description(
    "This enables round robin partitioning for source dataset, reffering the value from spark.sql.shuffle.partitions"
  ) var enable_round_robin_partioning: Boolean = false,
  @Description(
    "If true then will create encrypted ht2_idw file"
  ) var create_ht2_encrypt_file: String = "false",
  @Description(
    "Base location where insertload ready spark part file for ecrypted ht2_idw file is generated"
  ) var ht2_load_ready_insert_path: String =
    "dbfs:/mnt/prophecy_dev/ht2_load_ready",
  @Description(
    "Base location where single load ready ecrypted ht2_idw file is generated"
  ) var ht2_single_load_ready_path: String =
    "dbfs:/mnt/prophecy_dev/ht2_inbound",
  @Description(
    "if true then ht2_idw encrypted file will contain all columns which are output of reforamt rules"
  ) var ht2_extra_columns_reformat: String = "false",
  @Description(
    "Delta table to identify column names which needs to be encrypted"
  ) var ht2_encrypt_columns_meta_table: String = "metadata.ht2_encrypt_fields",
  @Description(
    "Delta table to identify column names which needs to be nullified"
  ) var ht2_nullify_columns_meta_table: String = "metadata.ht2_nullable_fields",
  @Description(
    "Flag to turn or or off optimize write flag for delta table"
  ) var optimize_write_flag: String = "true",
  @Description(
    "If true then main table load ready files would not be generated. Default: false"
  ) var skip_main_table_load_ready_files: String = "false",
  @Description(
    "If value is not None then this value would be used for locking and SK generation counter"
  ) var sk_table_name_override: String = "None",
  @Description(
    "If true then final table would be defaulted to decimal as per final table schema instead of double"
  ) var default_to_decimal_type: String = "false",
  @Description(
    "If true then final_output will be updated else always will refer the input dataframe. Default: false"
  ) var sk_placeholder_skip_final_override: String = "false",
  @Description(
    "Used for adding partition column explicitly while performing delta merge"
  ) var partition_column_explicit: String = "None",
  @Description(
    "Uses sk_service_base_url_key to fetch URL from key vault"
  ) var sk_service_base_url_key: String = "skServiceBaseURL",
  @Description("Key to fetch UID from key vault") var update_uid_key: String =
    "updateID",
  @Description(
    "Flag to be turned on for accumulated process for common dimensions"
  ) var accumulated_process_flag: String = "false",
  @Description(
    "Delta table where accumulated processed files list is maintained"
  ) var accumulated_processed_files_table: String =
    "temp.accumulated_processed_files_table",
  @Description(
    "Lookup column used for fetching src_env_rank"
  ) var survivorship_lookup_column: String = "src_env_sk",
  @Description(
    "Print DFs from apply_reformat_rules onwards"
  ) var debug_flag: Boolean = false,
  @Description(
    "{rint Intermediate Final Output DF from SK Service Placeholder (Very compute intensive)"
  ) var final_output_debug_flag: Boolean = false,
  @Description(
    "Filter Condition to Select records While printing intermediate DFs"
  ) var debug_filter: String = "None",
  @Description(
    "List of Columns to Select While printing intermediate DFs"
  ) var debug_col_list: String = "None",
  @Description(
    "Any columns not present in source that are present in final_table_schema would be filled in as NULL if this flag is enabled."
  ) var ph_source_column_nulls_if_not_present: Boolean = false,
  @Description(
    "For placeholders, insert_uid should come from source, not secrets or hardcoded"
  ) var get_insert_uid_from_source_df: Boolean = false,
  @Description(
    "The Maximum number of tries for regenerating SKs in case any NULLs get generated"
  ) var max_retries_tbl_sk_gen: Int = 1,
  @Description(
    "File path for saving final_output DF in case of NULL SK Generation"
  ) var retry_log_file_folder: String =
    "dbfs:/mnt/prophecy_dev/sk_gen_final_output/",
  @Description(
    "flag for handling -1 as sk col present in main target table"
  ) var enable_negative_one_self_join_sk: Boolean = false,
  @Description(
    "enable/disable optional header for csv, txt and dat files"
  ) var enable_optional_input_file_header: Boolean = true,
  @Description(
    "enforce final table schema while reading input files"
  ) var enable_read_schema_from_target_table: Boolean = false
) extends ConfigBase
