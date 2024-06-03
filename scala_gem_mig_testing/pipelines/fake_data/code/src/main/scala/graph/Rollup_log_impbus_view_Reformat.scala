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

object Rollup_log_impbus_view_Reformat {

  def apply(context: Context, in: DataFrame): DataFrame =
    in.select(
      col("date_time").cast(LongType).as("date_time"),
      col("auction_id_64").cast(LongType).as("auction_id_64"),
      col("user_id_64").cast(LongType).as("user_id_64"),
      col("view_result").cast(IntegerType).as("view_result"),
      col("ttl").cast(IntegerType).as("ttl"),
      col("view_data"),
      col("viewdef_definition_id")
        .cast(IntegerType)
        .as("viewdef_definition_id"),
      col("viewdef_view_result").cast(IntegerType).as("viewdef_view_result"),
      col("view_not_measurable_type")
        .cast(IntegerType)
        .as("view_not_measurable_type"),
      col("view_not_visible_type")
        .cast(IntegerType)
        .as("view_not_visible_type"),
      col("view_frame_type").cast(IntegerType).as("view_frame_type"),
      col("view_script_version").cast(IntegerType).as("view_script_version"),
      col("view_tag_version"),
      col("view_screen_width").cast(IntegerType).as("view_screen_width"),
      col("view_screen_height").cast(IntegerType).as("view_screen_height"),
      col("view_js_browser"),
      col("view_js_platform"),
      col("view_banner_left").cast(IntegerType).as("view_banner_left"),
      col("view_banner_top").cast(IntegerType).as("view_banner_top"),
      col("view_banner_width").cast(IntegerType).as("view_banner_width"),
      col("view_banner_height").cast(IntegerType).as("view_banner_height"),
      col("view_tracking_duration"),
      col("view_page_duration"),
      col("view_usage_duration"),
      col("view_surface"),
      col("view_js_message"),
      col("view_player_width").cast(IntegerType).as("view_player_width"),
      col("view_player_height").cast(IntegerType).as("view_player_height"),
      col("view_iab_duration"),
      col("view_iab_inview_count")
        .cast(IntegerType)
        .as("view_iab_inview_count"),
      col("view_duration_gt_0pct"),
      col("view_duration_gt_25pct"),
      col("view_duration_gt_50pct"),
      col("view_duration_gt_75pct"),
      col("view_duration_eq_100pct"),
      col("auction_timestamp").cast(LongType).as("auction_timestamp"),
      col("view_has_banner_left").cast(IntegerType).as("view_has_banner_left"),
      col("view_has_banner_top").cast(IntegerType).as("view_has_banner_top"),
      col("view_mouse_position_final_x")
        .cast(IntegerType)
        .as("view_mouse_position_final_x"),
      col("view_mouse_position_final_y")
        .cast(IntegerType)
        .as("view_mouse_position_final_y"),
      col("view_has_mouse_position_final")
        .cast(IntegerType)
        .as("view_has_mouse_position_final"),
      col("view_mouse_position_initial_x")
        .cast(IntegerType)
        .as("view_mouse_position_initial_x"),
      col("view_mouse_position_initial_y")
        .cast(IntegerType)
        .as("view_mouse_position_initial_y"),
      col("view_has_mouse_position_initial")
        .cast(IntegerType)
        .as("view_has_mouse_position_initial"),
      col("view_mouse_position_page_x")
        .cast(IntegerType)
        .as("view_mouse_position_page_x"),
      col("view_mouse_position_page_y")
        .cast(IntegerType)
        .as("view_mouse_position_page_y"),
      col("view_has_mouse_position_page")
        .cast(IntegerType)
        .as("view_has_mouse_position_page"),
      col("view_mouse_position_timeout_x")
        .cast(IntegerType)
        .as("view_mouse_position_timeout_x"),
      col("view_mouse_position_timeout_y")
        .cast(IntegerType)
        .as("view_mouse_position_timeout_y"),
      col("view_has_mouse_position_timeout")
        .cast(IntegerType)
        .as("view_has_mouse_position_timeout"),
      col("view_session_id").cast(LongType).as("view_session_id"),
      col("view_video"),
      col("anonymized_user_info"),
      col("is_deferred").cast(ByteType).as("is_deferred")
    )

}
