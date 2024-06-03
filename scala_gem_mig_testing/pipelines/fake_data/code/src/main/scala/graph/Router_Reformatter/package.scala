package graph

import io.prophecy.libs._
import udfs.PipelineInitCode._
import graph.Router_Reformatter.config._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._
package object Router_Reformatter {

  def apply(context: Context, in: DataFrame): Subgraph5 = {
    val (df_Reformat_TRAN_Router_Reformatter_RowDistributor_out0,
         df_Reformat_TRAN_Router_Reformatter_RowDistributor_out1,
         df_Reformat_TRAN_Router_Reformatter_RowDistributor_out2,
         df_Reformat_TRAN_Router_Reformatter_RowDistributor_out3,
         df_Reformat_TRAN_Router_Reformatter_RowDistributor_out4
    ) = Reformat_TRAN_Router_Reformatter_RowDistributor(context, in)
    val df_Reformat_TRAN_Router_ReformatterReformat_3 =
      Reformat_TRAN_Router_ReformatterReformat_3(
        context,
        df_Reformat_TRAN_Router_Reformatter_RowDistributor_out3
      )
    val df_Reformat_TRAN_Router_ReformatterReformat_4 =
      Reformat_TRAN_Router_ReformatterReformat_4(
        context,
        df_Reformat_TRAN_Router_Reformatter_RowDistributor_out4
      )
    val df_Reformat_TRAN_Router_ReformatterReformat_2 =
      Reformat_TRAN_Router_ReformatterReformat_2(
        context,
        df_Reformat_TRAN_Router_Reformatter_RowDistributor_out2
      )
    val df_Reformat_TRAN_Router_ReformatterReformat_1 =
      Reformat_TRAN_Router_ReformatterReformat_1(
        context,
        df_Reformat_TRAN_Router_Reformatter_RowDistributor_out1
      )
    val df_Reformat_TRAN_Router_ReformatterReformat_0 =
      Reformat_TRAN_Router_ReformatterReformat_0(
        context,
        df_Reformat_TRAN_Router_Reformatter_RowDistributor_out0
      )
    (df_Reformat_TRAN_Router_ReformatterReformat_3,
     df_Reformat_TRAN_Router_ReformatterReformat_0,
     df_Reformat_TRAN_Router_ReformatterReformat_2,
     df_Reformat_TRAN_Router_ReformatterReformat_4,
     df_Reformat_TRAN_Router_ReformatterReformat_1
    )
  }

}
