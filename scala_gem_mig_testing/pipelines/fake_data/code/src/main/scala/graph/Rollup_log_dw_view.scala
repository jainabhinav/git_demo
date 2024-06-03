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

object Rollup_log_dw_view {

  def apply(context: Context, in: DataFrame): DataFrame =
    in.groupBy(col("auction_id_64"))
      .agg(
        last(col("date_time").cast(LongType)).as("date_time"),
        collect_list(
          when(is_not_null(
                 col("view_auction_event_type")
                   .cast(BooleanType)
                   .and(col("view_auction_event_type") > lit(0))
               ),
               col("view_auction_event_type")
          )
        ).as("additional_clearing_events"),
        log_impbus_auction_event(context).as("log_impbus_auction_event"),
        last(
          when(
            is_not_null(col("view_event_type_id").cast(IntegerType))
              .and(col("view_event_type_id").cast(IntegerType) === lit(2)),
            struct(
              col("date_time").cast(LongType).as("date_time"),
              col("auction_id_64").cast(LongType).as("auction_id_64"),
              col("user_id_64").cast(LongType).as("user_id_64"),
              col("advertiser_currency"),
              col("advertiser_exchange_rate"),
              col("booked_revenue_dollars"),
              col("booked_revenue_adv_curr"),
              col("publisher_currency"),
              col("publisher_exchange_rate"),
              col("payment_value"),
              col("ip_address"),
              col("auction_timestamp").cast(LongType).as("auction_timestamp"),
              col("view_auction_event_type")
                .cast(IntegerType)
                .as("view_auction_event_type"),
              col("anonymized_user_info"),
              col("view_event_type_id")
                .cast(IntegerType)
                .as("view_event_type_id"),
              col("revenue_info"),
              col("use_revenue_info").cast(ByteType).as("use_revenue_info"),
              col("is_deferred").cast(ByteType).as("is_deferred"),
              col("ecpm_conversion_rate")
            )
          )
        ).as("log_dw_view")
      )

  def log_impbus_auction_event(context: Context) = {
    val spark  = context.spark
    val Config = context.config
    last(
      lit(null).cast(
        StructType(
          Array(
            StructField("date_time",                 LongType,    true),
            StructField("auction_id_64",             LongType,    true),
            StructField("payment_value_microcents",  LongType,    true),
            StructField("transaction_event",         IntegerType, true),
            StructField("transaction_event_type_id", IntegerType, true),
            StructField("is_deferred",               ByteType,    true),
            StructField(
              "auction_event_pricing",
              StructType(
                Array(
                  StructField("gross_payment_value_microcents", LongType, true),
                  StructField("net_payment_value_microcents",   LongType, true),
                  StructField("seller_revenue_microcents",      LongType, true),
                  StructField(
                    "buyer_charges",
                    StructType(
                      Array(
                        StructField("rate_card_id", IntegerType, true),
                        StructField("member_id",    IntegerType, true),
                        StructField("is_dw",        ByteType,    true),
                        StructField(
                          "pricing_terms",
                          ArrayType(
                            StructType(
                              Array(
                                StructField("term_id",      IntegerType, true),
                                StructField("amount",       DoubleType,  true),
                                StructField("rate",         DoubleType,  true),
                                StructField("is_deduction", ByteType,    true),
                                StructField("is_media_cost_dependent",
                                            ByteType,
                                            true
                                ),
                                StructField("data_member_id", IntegerType, true)
                              )
                            ),
                            true
                          ),
                          true
                        ),
                        StructField("fx_margin_rate_id",    IntegerType, true),
                        StructField("marketplace_owner_id", IntegerType, true),
                        StructField("virtual_marketplace_id",
                                    IntegerType,
                                    true
                        ),
                        StructField("amino_enabled", ByteType, true)
                      )
                    ),
                    true
                  ),
                  StructField(
                    "seller_charges",
                    StructType(
                      Array(
                        StructField("rate_card_id", IntegerType, true),
                        StructField("member_id",    IntegerType, true),
                        StructField("is_dw",        ByteType,    true),
                        StructField(
                          "pricing_terms",
                          ArrayType(
                            StructType(
                              Array(
                                StructField("term_id",      IntegerType, true),
                                StructField("amount",       DoubleType,  true),
                                StructField("rate",         DoubleType,  true),
                                StructField("is_deduction", ByteType,    true),
                                StructField("is_media_cost_dependent",
                                            ByteType,
                                            true
                                ),
                                StructField("data_member_id", IntegerType, true)
                              )
                            ),
                            true
                          ),
                          true
                        ),
                        StructField("fx_margin_rate_id",    IntegerType, true),
                        StructField("marketplace_owner_id", IntegerType, true),
                        StructField("virtual_marketplace_id",
                                    IntegerType,
                                    true
                        ),
                        StructField("amino_enabled", ByteType, true)
                      )
                    ),
                    true
                  ),
                  StructField("buyer_transacted",  ByteType, true),
                  StructField("seller_transacted", ByteType, true)
                )
              ),
              true
            )
          )
        )
      )
    )
  }

}
