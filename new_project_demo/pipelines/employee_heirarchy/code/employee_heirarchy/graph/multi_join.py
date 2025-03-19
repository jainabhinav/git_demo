from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from employee_heirarchy.config.ConfigStore import *
from employee_heirarchy.functions import *

def multi_join(
        spark: SparkSession,
        in0: DataFrame,
        in1: DataFrame,
        in2: DataFrame, 
        in3: DataFrame, 
        in4: DataFrame, 
        in5: DataFrame, 
        in6: DataFrame, 
        in7: DataFrame, 
        in8: DataFrame, 
        in9: DataFrame, 
        in10: DataFrame, 
        in11: DataFrame, 
        in12: DataFrame, 
        in13: DataFrame, 
        in14: DataFrame, 
        in15: DataFrame, 
        in16: DataFrame, 
        in17: DataFrame, 
        in18: DataFrame, 
        in19: DataFrame
) -> DataFrame:
    return in0\
        .alias("in0")\
        .join(in1.alias("in1"), (col("in0.EM_USER_ID") == col("in1.USER_ID")), "left_outer")\
        .join(in2.alias("in2"), (col("in1.EM_USER_ID") == col("in2.USER_ID")), "left_outer")\
        .join(in3.alias("in3"), (col("in2.EM_USER_ID") == col("in3.USER_ID")), "left_outer")\
        .join(in4.alias("in4"), (col("in3.EM_USER_ID") == col("in4.USER_ID")), "left_outer")\
        .join(in5.alias("in5"), (col("in4.EM_USER_ID") == col("in5.USER_ID")), "left_outer")\
        .join(in6.alias("in6"), (col("in5.EM_USER_ID") == col("in6.USER_ID")), "left_outer")\
        .join(in7.alias("in7"), (col("in6.EM_USER_ID") == col("in7.USER_ID")), "left_outer")\
        .join(in8.alias("in8"), (col("in7.EM_USER_ID") == col("in8.USER_ID")), "left_outer")\
        .join(in9.alias("in9"), (col("in8.EM_USER_ID") == col("in9.USER_ID")), "left_outer")\
        .join(in10.alias("in10"), (col("in9.EM_USER_ID") == col("in10.USER_ID")), "left_outer")\
        .join(in11.alias("in11"), (col("in10.EM_USER_ID") == col("in11.USER_ID")), "left_outer")\
        .join(in12.alias("in12"), (col("in11.EM_USER_ID") == col("in12.USER_ID")), "left_outer")\
        .join(in13.alias("in13"), (col("in12.EM_USER_ID") == col("in13.USER_ID")), "left_outer")\
        .join(in14.alias("in14"), (col("in13.EM_USER_ID") == col("in14.USER_ID")), "left_outer")\
        .join(in15.alias("in15"), (col("in14.EM_USER_ID") == col("in15.USER_ID")), "left_outer")\
        .join(in16.alias("in16"), (col("in15.EM_USER_ID") == col("in16.USER_ID")), "left_outer")\
        .join(in17.alias("in17"), (col("in16.EM_USER_ID") == col("in17.USER_ID")), "left_outer")\
        .join(in18.alias("in18"), (col("in17.EM_USER_ID") == col("in18.USER_ID")), "left_outer")\
        .join(in19.alias("in19"), (col("in18.EM_USER_ID") == col("in19.USER_ID")), "left_outer")\
        .select(col("in0.AUX_SRC_SYS").alias("AUX_SRC_SYS"), col("in0.AUX_INS_DTM").alias("AUX_INS_DTM"), col("in0.AUX_SRC_STATUS").alias("AUX_SRC_STATUS"), col("in0.POS_NUM").alias("POS_NUM"), col("in0.POS_TITLE").alias("POS_TITLE"), col("in0.EM_POS_NUM").alias("EM_POS_NUM"), filter(
          reverse(
            array(
              struct(
                col("in0.POS_NUM").alias("POS_NUM"), 
                col("in0.POS_TITLE").alias("POS_TITLE"), 
                col("in0.EMPL_ID").alias("EMPL_ID"), 
                col("in0.EMPL_NAME").alias("EMPL_NAME")
              ), 
              struct(
                col("in1.POS_NUM").alias("POS_NUM"), 
                col("in1.POS_TITLE").alias("POS_TITLE"), 
                col("in1.EMPL_ID").alias("EMPL_ID"), 
                col("in1.EMPL_NAME").alias("EMPL_NAME")
              ), 
              struct(
                col("in2.POS_NUM").alias("POS_NUM"), 
                col("in2.POS_TITLE").alias("POS_TITLE"), 
                col("in2.EMPL_ID").alias("EMPL_ID"), 
                col("in2.EMPL_NAME").alias("EMPL_NAME")
              ), 
              struct(
                col("in3.POS_NUM").alias("POS_NUM"), 
                col("in3.POS_TITLE").alias("POS_TITLE"), 
                col("in3.EMPL_ID").alias("EMPL_ID"), 
                col("in3.EMPL_NAME").alias("EMPL_NAME")
              ), 
              struct(
                col("in4.POS_NUM").alias("POS_NUM"), 
                col("in4.POS_TITLE").alias("POS_TITLE"), 
                col("in4.EMPL_ID").alias("EMPL_ID"), 
                col("in4.EMPL_NAME").alias("EMPL_NAME")
              ), 
              struct(
                col("in5.POS_NUM").alias("POS_NUM"), 
                col("in5.POS_TITLE").alias("POS_TITLE"), 
                col("in5.EMPL_ID").alias("EMPL_ID"), 
                col("in5.EMPL_NAME").alias("EMPL_NAME")
              ), 
              struct(
                col("in6.POS_NUM").alias("POS_NUM"), 
                col("in6.POS_TITLE").alias("POS_TITLE"), 
                col("in6.EMPL_ID").alias("EMPL_ID"), 
                col("in6.EMPL_NAME").alias("EMPL_NAME")
              ), 
              struct(
                col("in7.POS_NUM").alias("POS_NUM"), 
                col("in7.POS_TITLE").alias("POS_TITLE"), 
                col("in7.EMPL_ID").alias("EMPL_ID"), 
                col("in7.EMPL_NAME").alias("EMPL_NAME")
              ), 
              struct(
                col("in8.POS_NUM").alias("POS_NUM"), 
                col("in8.POS_TITLE").alias("POS_TITLE"), 
                col("in8.EMPL_ID").alias("EMPL_ID"), 
                col("in8.EMPL_NAME").alias("EMPL_NAME")
              ), 
              struct(
                col("in9.POS_NUM").alias("POS_NUM"), 
                col("in9.POS_TITLE").alias("POS_TITLE"), 
                col("in9.EMPL_ID").alias("EMPL_ID"), 
                col("in9.EMPL_NAME").alias("EMPL_NAME")
              ), 
              struct(
                col("in10.POS_NUM").alias("POS_NUM"), 
                col("in10.POS_TITLE").alias("POS_TITLE"), 
                col("in10.EMPL_ID").alias("EMPL_ID"), 
                col("in10.EMPL_NAME").alias("EMPL_NAME")
              ), 
              struct(
                col("in11.POS_NUM").alias("POS_NUM"), 
                col("in11.POS_TITLE").alias("POS_TITLE"), 
                col("in11.EMPL_ID").alias("EMPL_ID"), 
                col("in11.EMPL_NAME").alias("EMPL_NAME")
              ), 
              struct(
                col("in12.POS_NUM").alias("POS_NUM"), 
                col("in12.POS_TITLE").alias("POS_TITLE"), 
                col("in12.EMPL_ID").alias("EMPL_ID"), 
                col("in12.EMPL_NAME").alias("EMPL_NAME")
              ), 
              struct(
                col("in13.POS_NUM").alias("POS_NUM"), 
                col("in13.POS_TITLE").alias("POS_TITLE"), 
                col("in13.EMPL_ID").alias("EMPL_ID"), 
                col("in13.EMPL_NAME").alias("EMPL_NAME")
              ), 
              struct(
                col("in14.POS_NUM").alias("POS_NUM"), 
                col("in14.POS_TITLE").alias("POS_TITLE"), 
                col("in14.EMPL_ID").alias("EMPL_ID"), 
                col("in14.EMPL_NAME").alias("EMPL_NAME")
              ), 
              struct(
                col("in15.POS_NUM").alias("POS_NUM"), 
                col("in15.POS_TITLE").alias("POS_TITLE"), 
                col("in15.EMPL_ID").alias("EMPL_ID"), 
                col("in15.EMPL_NAME").alias("EMPL_NAME")
              ), 
              struct(
                col("in16.POS_NUM").alias("POS_NUM"), 
                col("in16.POS_TITLE").alias("POS_TITLE"), 
                col("in16.EMPL_ID").alias("EMPL_ID"), 
                col("in16.EMPL_NAME").alias("EMPL_NAME")
              ), 
              struct(
                col("in17.POS_NUM").alias("POS_NUM"), 
                col("in17.POS_TITLE").alias("POS_TITLE"), 
                col("in17.EMPL_ID").alias("EMPL_ID"), 
                col("in17.EMPL_NAME").alias("EMPL_NAME")
              ), 
              struct(
                col("in18.POS_NUM").alias("POS_NUM"), 
                col("in18.POS_TITLE").alias("POS_TITLE"), 
                col("in18.EMPL_ID").alias("EMPL_ID"), 
                col("in18.EMPL_NAME").alias("EMPL_NAME")
              ), 
              struct(
                col("in19.POS_NUM").alias("POS_NUM"), 
                col("in19.POS_TITLE").alias("POS_TITLE"), 
                col("in19.EMPL_ID").alias("EMPL_ID"), 
                col("in19.EMPL_NAME").alias("EMPL_NAME")
              )
            )
          ), 
          lambda x: x.getField("EMPL_ID").isNotNull()
        )\
        .alias("HEIRARCHY_ARR"))
