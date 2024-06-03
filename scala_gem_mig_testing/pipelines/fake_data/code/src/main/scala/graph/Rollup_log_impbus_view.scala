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

object Rollup_log_impbus_view {

  def apply(context: Context, in: DataFrame): DataFrame =
    in.groupBy(col("auction_id_64"))
      .agg(
        lit(null).cast(LongType).as("date_time"),
        lit(null).cast(LongType).as("user_id_64"),
        rollup1615_UDF(collect_list(col("view_result")), lit(null))
          .as("view_result"),
        lit(null).cast(IntegerType).as("ttl"),
        lit(null).cast(StringType).as("view_data"),
        rollup1621_UDF(collect_list(col("viewdef_definition_id")), lit(null))
          .as("viewdef_definition_id"),
        rollup1627_UDF(collect_list(col("viewdef_view_result")),
                       collect_list(col("viewdef_definition_id")),
                       lit(null),
                       lit(null)
        ).as("viewdef_view_result"),
        lit(null).cast(IntegerType).as("view_not_measurable_type"),
        lit(null).cast(IntegerType).as("view_not_visible_type"),
        lit(null).cast(IntegerType).as("view_frame_type"),
        lit(null).cast(IntegerType).as("view_script_version"),
        lit(null).cast(StringType).as("view_tag_version"),
        lit(null).cast(IntegerType).as("view_screen_width"),
        lit(null).cast(IntegerType).as("view_screen_height"),
        lit(null).cast(StringType).as("view_js_browser"),
        lit(null).cast(StringType).as("view_js_platform"),
        lit(null).cast(IntegerType).as("view_banner_left"),
        lit(null).cast(IntegerType).as("view_banner_top"),
        lit(null).cast(IntegerType).as("view_banner_width"),
        lit(null).cast(IntegerType).as("view_banner_height"),
        lit(null).cast(DoubleType).as("view_tracking_duration"),
        lit(null).cast(DoubleType).as("view_page_duration"),
        lit(null).cast(DoubleType).as("view_usage_duration"),
        lit(null).cast(DoubleType).as("view_surface"),
        lit(null).cast(StringType).as("view_js_message"),
        lit(null).cast(IntegerType).as("view_player_width"),
        lit(null).cast(IntegerType).as("view_player_height"),
        lit(null).cast(DoubleType).as("view_iab_duration"),
        lit(null).cast(IntegerType).as("view_iab_inview_count"),
        lit(null).cast(DoubleType).as("view_duration_gt_0pct"),
        lit(null).cast(DoubleType).as("view_duration_gt_25pct"),
        lit(null).cast(DoubleType).as("view_duration_gt_50pct"),
        lit(null).cast(DoubleType).as("view_duration_gt_75pct"),
        lit(null).cast(DoubleType).as("view_duration_eq_100pct"),
        lit(null).cast(LongType).as("auction_timestamp"),
        lit(null).cast(IntegerType).as("view_has_banner_left"),
        lit(null).cast(IntegerType).as("view_has_banner_top"),
        lit(null).cast(IntegerType).as("view_mouse_position_final_x"),
        lit(null).cast(IntegerType).as("view_mouse_position_final_y"),
        lit(null).cast(IntegerType).as("view_has_mouse_position_final"),
        lit(null).cast(IntegerType).as("view_mouse_position_initial_x"),
        lit(null).cast(IntegerType).as("view_mouse_position_initial_y"),
        lit(null).cast(IntegerType).as("view_has_mouse_position_initial"),
        lit(null).cast(IntegerType).as("view_mouse_position_page_x"),
        lit(null).cast(IntegerType).as("view_mouse_position_page_y"),
        lit(null).cast(IntegerType).as("view_has_mouse_position_page"),
        lit(null).cast(IntegerType).as("view_mouse_position_timeout_x"),
        lit(null).cast(IntegerType).as("view_mouse_position_timeout_y"),
        lit(null).cast(IntegerType).as("view_has_mouse_position_timeout"),
        lit(null).cast(LongType).as("view_session_id"),
        last(col("view_video")).as("view_video"),
        last(col("anonymized_user_info")).as("anonymized_user_info"),
        lit(null).cast(ByteType).as("is_deferred")
      )

}
