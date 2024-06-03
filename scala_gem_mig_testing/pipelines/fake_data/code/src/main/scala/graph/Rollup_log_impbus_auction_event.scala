package graph

import io.prophecy.libs._
import udfs.PipelineInitCode._
import udfs.UDFs._
import config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Rollup_log_impbus_auction_event {

  def apply(context: Context, in: DataFrame): DataFrame =
    in.groupBy(col("auction_id_64"))
      .agg(
        last(col("date_time").cast(LongType)).as("date_time"),
        collect_list(
          when(is_not_null(
                 col("transaction_event")
                   .cast(BooleanType)
                   .and(col("transaction_event") > lit(0))
               ),
               col("transaction_event")
          )
        ).as("additional_clearing_events"),
        last(
          when(
            is_not_null(col("transaction_event_type_id").cast(IntegerType)).and(
              col("transaction_event_type_id").cast(IntegerType) === lit(2)
            ),
            struct(
              col("date_time").cast(LongType).as("date_time"),
              col("auction_id_64").cast(LongType).as("auction_id_64"),
              col("payment_value_microcents")
                .cast(LongType)
                .as("payment_value_microcents"),
              col("transaction_event")
                .cast(IntegerType)
                .as("transaction_event"),
              col("transaction_event_type_id")
                .cast(IntegerType)
                .as("transaction_event_type_id"),
              col("is_deferred").cast(ByteType).as("is_deferred"),
              col("auction_event_pricing")
            )
          )
        ).as("log_impbus_auction_event"),
        log_dw_view(context).as("log_dw_view")
      )

  def log_dw_view(context: Context) = {
    val spark  = context.spark
    val Config = context.config
    last(
      lit(null).cast(
        StructType(
          Array(
            StructField("date_time",                LongType,    true),
            StructField("auction_id_64",            LongType,    true),
            StructField("user_id_64",               LongType,    true),
            StructField("advertiser_currency",      StringType,  true),
            StructField("advertiser_exchange_rate", DoubleType,  true),
            StructField("booked_revenue_dollars",   DoubleType,  true),
            StructField("booked_revenue_adv_curr",  DoubleType,  true),
            StructField("publisher_currency",       StringType,  true),
            StructField("publisher_exchange_rate",  DoubleType,  true),
            StructField("payment_value",            DoubleType,  true),
            StructField("ip_address",               StringType,  true),
            StructField("auction_timestamp",        LongType,    true),
            StructField("view_auction_event_type",  IntegerType, true),
            StructField(
              "anonymized_user_info",
              StructType(Array(StructField("user_id", BinaryType, true))),
              true
            ),
            StructField("view_event_type_id", IntegerType, true),
            StructField(
              "revenue_info",
              StructType(
                Array(
                  StructField("total_partner_fees_microcents", LongType,   true),
                  StructField("booked_revenue_dollars",        DoubleType, true),
                  StructField("booked_revenue_adv_curr",       DoubleType, true),
                  StructField("total_data_costs_microcents",   LongType,   true),
                  StructField("total_profit_microcents",       LongType,   true),
                  StructField("total_segment_data_costs_microcents",
                              LongType,
                              true
                  ),
                  StructField("total_feature_costs_microcents", LongType, true)
                )
              ),
              true
            ),
            StructField("use_revenue_info",     ByteType,   true),
            StructField("is_deferred",          ByteType,   true),
            StructField("ecpm_conversion_rate", DoubleType, true)
          )
        )
      )
    )
  }

}
