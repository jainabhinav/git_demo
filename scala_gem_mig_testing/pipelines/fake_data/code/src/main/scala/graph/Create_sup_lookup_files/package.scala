package graph

import io.prophecy.libs._
import udfs.PipelineInitCode._
import graph.Create_sup_lookup_files.config._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._
package object Create_sup_lookup_files {

  def apply(context: Context): Unit = {
    val df_Read_Proto_Range_sup_bidder_member_sales_tax_rate_pb_sup_bidder_member_sales_tax_rate =
      Read_Proto_Range_sup_bidder_member_sales_tax_rate_pb_sup_bidder_member_sales_tax_rate(
        context
      )
    val df_Filter_by_Expression_6 = Filter_by_Expression_6(
      context,
      df_Read_Proto_Range_sup_bidder_member_sales_tax_rate_pb_sup_bidder_member_sales_tax_rate
    )
    val df_Sort_sup_bidder_member_sales_tax_rate_pb_sup_bidder_member_sales_tax_rate =
      Sort_sup_bidder_member_sales_tax_rate_pb_sup_bidder_member_sales_tax_rate(
        context,
        df_Filter_by_Expression_6
      )
    val df_Rollup_sup_bidder_member_sales_tax_rate_pb_sup_bidder_member_sales_tax_rate =
      Rollup_sup_bidder_member_sales_tax_rate_pb_sup_bidder_member_sales_tax_rate(
        context,
        df_Sort_sup_bidder_member_sales_tax_rate_pb_sup_bidder_member_sales_tax_rate
      )
    val df_Rollup_sup_bidder_member_sales_tax_rate_pb_sup_bidder_member_sales_tax_rate_Reformat =
      Rollup_sup_bidder_member_sales_tax_rate_pb_sup_bidder_member_sales_tax_rate_Reformat(
        context,
        df_Rollup_sup_bidder_member_sales_tax_rate_pb_sup_bidder_member_sales_tax_rate
      )
    sup_bidder_member_sales_tax_rate_lookup(
      context,
      df_Rollup_sup_bidder_member_sales_tax_rate_pb_sup_bidder_member_sales_tax_rate_Reformat
    )
    val df_Read_Proto_Range_sup_common_deal_pb_sup_common_deal =
      Read_Proto_Range_sup_common_deal_pb_sup_common_deal(context)
    val df_Sort_sup_common_deal_pb_sup_common_deal =
      Sort_sup_common_deal_pb_sup_common_deal(
        context,
        df_Read_Proto_Range_sup_common_deal_pb_sup_common_deal
      )
    val df_Rollup_sup_common_deal_pb_sup_common_deal =
      Rollup_sup_common_deal_pb_sup_common_deal(
        context,
        df_Sort_sup_common_deal_pb_sup_common_deal
      )
    val df_Rollup_sup_common_deal_pb_sup_common_deal_Reformat =
      Rollup_sup_common_deal_pb_sup_common_deal_Reformat(
        context,
        df_Rollup_sup_common_deal_pb_sup_common_deal
      )
    sup_common_deal_lookup(context,
                           df_Rollup_sup_common_deal_pb_sup_common_deal_Reformat
    )
    val df_Read_Proto_Range_sup_sell_side_object_hierarchy_pb_sup_sell_side_object_hierarchy =
      Read_Proto_Range_sup_sell_side_object_hierarchy_pb_sup_sell_side_object_hierarchy(
        context
      )
    val df_Sort_sup_sell_side_object_hierarchy_pb_site_id =
      Sort_sup_sell_side_object_hierarchy_pb_site_id(
        context,
        df_Read_Proto_Range_sup_sell_side_object_hierarchy_pb_sup_sell_side_object_hierarchy
      )
    val df_Rollup_sup_sell_side_object_hierarchy_pb_site_id =
      Rollup_sup_sell_side_object_hierarchy_pb_site_id(
        context,
        df_Sort_sup_sell_side_object_hierarchy_pb_site_id
      )
    val df_Rollup_sup_sell_side_object_hierarchy_pb_site_id_Reformat =
      Rollup_sup_sell_side_object_hierarchy_pb_site_id_Reformat(
        context,
        df_Rollup_sup_sell_side_object_hierarchy_pb_site_id
      )
    publisher_id_by_site_id_lookup(
      context,
      df_Rollup_sup_sell_side_object_hierarchy_pb_site_id_Reformat
    )
    val df_Sort_sup_sell_side_object_hierarchy_pb_publisher_id =
      Sort_sup_sell_side_object_hierarchy_pb_publisher_id(
        context,
        df_Read_Proto_Range_sup_sell_side_object_hierarchy_pb_sup_sell_side_object_hierarchy
      )
    val df_Rollup_sup_sell_side_object_hierarchy_pb_publisher_id =
      Rollup_sup_sell_side_object_hierarchy_pb_publisher_id(
        context,
        df_Sort_sup_sell_side_object_hierarchy_pb_publisher_id
      )
    val df_Rollup_sup_sell_side_object_hierarchy_pb_publisher_id_Reformat =
      Rollup_sup_sell_side_object_hierarchy_pb_publisher_id_Reformat(
        context,
        df_Rollup_sup_sell_side_object_hierarchy_pb_publisher_id
      )
    member_id_by_publisher_id_lookup(
      context,
      df_Rollup_sup_sell_side_object_hierarchy_pb_publisher_id_Reformat
    )
    val df_Read_Proto_Range_sup_buy_side_object_hierarchy_pb_sup_buy_side_object_hierarchy =
      Read_Proto_Range_sup_buy_side_object_hierarchy_pb_sup_buy_side_object_hierarchy(
        context
      )
    val df_Sort_sup_buy_side_object_hierarchy_pb_campaign_id =
      Sort_sup_buy_side_object_hierarchy_pb_campaign_id(
        context,
        df_Read_Proto_Range_sup_buy_side_object_hierarchy_pb_sup_buy_side_object_hierarchy
      )
    val df_Rollup_sup_buy_side_object_hierarchy_pb_campaign_id =
      Rollup_sup_buy_side_object_hierarchy_pb_campaign_id(
        context,
        df_Sort_sup_buy_side_object_hierarchy_pb_campaign_id
      )
    val df_Rollup_sup_buy_side_object_hierarchy_pb_campaign_id_Reformat =
      Rollup_sup_buy_side_object_hierarchy_pb_campaign_id_Reformat(
        context,
        df_Rollup_sup_buy_side_object_hierarchy_pb_campaign_id
      )
    advertiser_id_by_campaign_id_lookup(
      context,
      df_Rollup_sup_buy_side_object_hierarchy_pb_campaign_id_Reformat
    )
    val df_Sort_sup_buy_side_object_hierarchy_pb_advertiser_id =
      Sort_sup_buy_side_object_hierarchy_pb_advertiser_id(
        context,
        df_Read_Proto_Range_sup_buy_side_object_hierarchy_pb_sup_buy_side_object_hierarchy
      )
    val df_Rollup_sup_buy_side_object_hierarchy_pb_advertiser_id =
      Rollup_sup_buy_side_object_hierarchy_pb_advertiser_id(
        context,
        df_Sort_sup_buy_side_object_hierarchy_pb_advertiser_id
      )
    val df_Rollup_sup_buy_side_object_hierarchy_pb_advertiser_id_Reformat =
      Rollup_sup_buy_side_object_hierarchy_pb_advertiser_id_Reformat(
        context,
        df_Rollup_sup_buy_side_object_hierarchy_pb_advertiser_id
      )
    member_id_by_advertiser_id_lookup(
      context,
      df_Rollup_sup_buy_side_object_hierarchy_pb_advertiser_id_Reformat
    )
    val df_Read_Proto_Range_sup_bidder_campaign_pb_sup_bidder_campaign =
      Read_Proto_Range_sup_bidder_campaign_pb_sup_bidder_campaign(context)
    val df_Sort_sup_bidder_campaign_pb_sup_bidder_campaign =
      Sort_sup_bidder_campaign_pb_sup_bidder_campaign(
        context,
        df_Read_Proto_Range_sup_bidder_campaign_pb_sup_bidder_campaign
      )
    val df_Rollup_sup_bidder_campaign_pb_sup_bidder_campaign =
      Rollup_sup_bidder_campaign_pb_sup_bidder_campaign(
        context,
        df_Sort_sup_bidder_campaign_pb_sup_bidder_campaign
      )
    val df_Rollup_sup_bidder_campaign_pb_sup_bidder_campaign_Reformat =
      Rollup_sup_bidder_campaign_pb_sup_bidder_campaign_Reformat(
        context,
        df_Rollup_sup_bidder_campaign_pb_sup_bidder_campaign
      )
    sup_bidder_campaign_lookup(
      context,
      df_Rollup_sup_bidder_campaign_pb_sup_bidder_campaign_Reformat
    )
    val df_Read_Proto_Range_sup_placement_video_attributes_pb_sup_placement_video_attributes =
      Read_Proto_Range_sup_placement_video_attributes_pb_sup_placement_video_attributes(
        context
      )
    val df_Filter_by_Expression_video_context_not_0 =
      Filter_by_Expression_video_context_not_0(
        context,
        df_Read_Proto_Range_sup_placement_video_attributes_pb_sup_placement_video_attributes
      )
    val df_Sort_sup_placement_video_attributes_pb_sup_placement_video_attributes =
      Sort_sup_placement_video_attributes_pb_sup_placement_video_attributes(
        context,
        df_Filter_by_Expression_video_context_not_0
      )
    val df_Rollup_sup_placement_video_attributes_pb_sup_placement_video_attributes =
      Rollup_sup_placement_video_attributes_pb_sup_placement_video_attributes(
        context,
        df_Sort_sup_placement_video_attributes_pb_sup_placement_video_attributes
      )
    val df_Rollup_sup_placement_video_attributes_pb_sup_placement_video_attributes_Reformat = Rollup_sup_placement_video_attributes_pb_sup_placement_video_attributes_Reformat(
      context,
      df_Rollup_sup_placement_video_attributes_pb_sup_placement_video_attributes
    )
    sup_placement_video_attributes_pb_lookup(
      context,
      df_Rollup_sup_placement_video_attributes_pb_sup_placement_video_attributes_Reformat
    )
    val df_Read_Proto_Range_sup_ip_range_pb_sup_ip_range =
      Read_Proto_Range_sup_ip_range_pb_sup_ip_range(context)
    val df_Filter_non_IPv4_address_and_IPv4_Business_IP_s =
      Filter_non_IPv4_address_and_IPv4_Business_IP_s(
        context,
        df_Read_Proto_Range_sup_ip_range_pb_sup_ip_range
      )
    val df_Reformat_sup_ip_range_pb_sup_ip_range =
      Reformat_sup_ip_range_pb_sup_ip_range(
        context,
        df_Filter_non_IPv4_address_and_IPv4_Business_IP_s
      )
    val df_Sort_sup_ip_range_pb_sup_ip_range =
      Sort_sup_ip_range_pb_sup_ip_range(context,
                                        df_Reformat_sup_ip_range_pb_sup_ip_range
      )
    val df_Rollup_sup_ip_range_pb_sup_ip_range =
      Rollup_sup_ip_range_pb_sup_ip_range(context,
                                          df_Sort_sup_ip_range_pb_sup_ip_range
      )
    val df_Rollup_sup_ip_range_pb_sup_ip_range_Reformat =
      Rollup_sup_ip_range_pb_sup_ip_range_Reformat(
        context,
        df_Rollup_sup_ip_range_pb_sup_ip_range
      )
    sup_ip_range_lookup(context,
                        df_Rollup_sup_ip_range_pb_sup_ip_range_Reformat
    )
    val df_Sort_sup_buy_side_object_hierarchy_pb_campaign_group_id =
      Sort_sup_buy_side_object_hierarchy_pb_campaign_group_id(
        context,
        df_Read_Proto_Range_sup_buy_side_object_hierarchy_pb_sup_buy_side_object_hierarchy
      )
    val df_Rollup_sup_buy_side_object_hierarchy_pb_campaign_group_id =
      Rollup_sup_buy_side_object_hierarchy_pb_campaign_group_id(
        context,
        df_Sort_sup_buy_side_object_hierarchy_pb_campaign_group_id
      )
    val df_Rollup_sup_buy_side_object_hierarchy_pb_campaign_group_id_Reformat =
      Rollup_sup_buy_side_object_hierarchy_pb_campaign_group_id_Reformat(
        context,
        df_Rollup_sup_buy_side_object_hierarchy_pb_campaign_group_id
      )
    advertiser_id_by_campaign_group_id_lookup(
      context,
      df_Rollup_sup_buy_side_object_hierarchy_pb_campaign_group_id_Reformat
    )
    val df_Read_Proto_Range_sup_tl_api_inventory_url_pb_sup_tl_api_inventory_url =
      Read_Proto_Range_sup_tl_api_inventory_url_pb_sup_tl_api_inventory_url(
        context
      )
    val df_Sort_sup_tl_api_inventory_url_pb_inventory_url_id =
      Sort_sup_tl_api_inventory_url_pb_inventory_url_id(
        context,
        df_Read_Proto_Range_sup_tl_api_inventory_url_pb_sup_tl_api_inventory_url
      )
    val df_Rollup_sup_tl_api_inventory_url_pb_inventory_url_id =
      Rollup_sup_tl_api_inventory_url_pb_inventory_url_id(
        context,
        df_Sort_sup_tl_api_inventory_url_pb_inventory_url_id
      )
    val df_Rollup_sup_tl_api_inventory_url_pb_inventory_url_id_Reformat =
      Rollup_sup_tl_api_inventory_url_pb_inventory_url_id_Reformat(
        context,
        df_Rollup_sup_tl_api_inventory_url_pb_inventory_url_id
      )
    inventory_url_by_id_lookup(
      context,
      df_Rollup_sup_tl_api_inventory_url_pb_inventory_url_id_Reformat
    )
    val df_Sort_sup_sell_side_object_hierarchy_pb_tag_id =
      Sort_sup_sell_side_object_hierarchy_pb_tag_id(
        context,
        df_Read_Proto_Range_sup_sell_side_object_hierarchy_pb_sup_sell_side_object_hierarchy
      )
    val df_Rollup_sup_sell_side_object_hierarchy_pb_tag_id =
      Rollup_sup_sell_side_object_hierarchy_pb_tag_id(
        context,
        df_Sort_sup_sell_side_object_hierarchy_pb_tag_id
      )
    val df_Rollup_sup_sell_side_object_hierarchy_pb_tag_id_Reformat =
      Rollup_sup_sell_side_object_hierarchy_pb_tag_id_Reformat(
        context,
        df_Rollup_sup_sell_side_object_hierarchy_pb_tag_id
      )
    publisher_id_by_tag_id_lookup(
      context,
      df_Rollup_sup_sell_side_object_hierarchy_pb_tag_id_Reformat
    )
    val df_Read_Proto_Range_sup_bidder_insertion_order_pb_sup_bidder_insertion_order =
      Read_Proto_Range_sup_bidder_insertion_order_pb_sup_bidder_insertion_order(
        context
      )
    val df_Sort_sup_bidder_insertion_order_pb_insertion_order_id =
      Sort_sup_bidder_insertion_order_pb_insertion_order_id(
        context,
        df_Read_Proto_Range_sup_bidder_insertion_order_pb_sup_bidder_insertion_order
      )
    val df_Rollup_sup_bidder_insertion_order_pb_insertion_order_id =
      Rollup_sup_bidder_insertion_order_pb_insertion_order_id(
        context,
        df_Sort_sup_bidder_insertion_order_pb_insertion_order_id
      )
    val df_Rollup_sup_bidder_insertion_order_pb_insertion_order_id_Reformat =
      Rollup_sup_bidder_insertion_order_pb_insertion_order_id_Reformat(
        context,
        df_Rollup_sup_bidder_insertion_order_pb_insertion_order_id
      )
    advertiser_id_by_insertion_order_id_lookup(
      context,
      df_Rollup_sup_bidder_insertion_order_pb_insertion_order_id_Reformat
    )
    advertiser_id_by_insertion_order_id(
      context,
      df_Rollup_sup_bidder_insertion_order_pb_insertion_order_id_Reformat
    )
    sup_bidder_campaign(
      context,
      df_Rollup_sup_bidder_campaign_pb_sup_bidder_campaign_Reformat
    )
    sup_placement_video_attributes_pb(
      context,
      df_Rollup_sup_placement_video_attributes_pb_sup_placement_video_attributes_Reformat
    )
    sup_bidder_member_sales_tax_rate(
      context,
      df_Rollup_sup_bidder_member_sales_tax_rate_pb_sup_bidder_member_sales_tax_rate_Reformat
    )
    publisher_id_by_site_id(
      context,
      df_Rollup_sup_sell_side_object_hierarchy_pb_site_id_Reformat
    )
    inventory_url_by_id(
      context,
      df_Rollup_sup_tl_api_inventory_url_pb_inventory_url_id_Reformat
    )
    member_id_by_advertiser_id(
      context,
      df_Rollup_sup_buy_side_object_hierarchy_pb_advertiser_id_Reformat
    )
    member_id_by_publisher_id(
      context,
      df_Rollup_sup_sell_side_object_hierarchy_pb_publisher_id_Reformat
    )
    advertiser_id_by_campaign_group_id(
      context,
      df_Rollup_sup_buy_side_object_hierarchy_pb_campaign_group_id_Reformat
    )
    sup_common_deal(context,
                    df_Rollup_sup_common_deal_pb_sup_common_deal_Reformat
    )
    advertiser_id_by_campaign_id(
      context,
      df_Rollup_sup_buy_side_object_hierarchy_pb_campaign_id_Reformat
    )
    sup_ip_range(context, df_Rollup_sup_ip_range_pb_sup_ip_range_Reformat)
    publisher_id_by_tag_id(
      context,
      df_Rollup_sup_sell_side_object_hierarchy_pb_tag_id_Reformat
    )
  }

}
