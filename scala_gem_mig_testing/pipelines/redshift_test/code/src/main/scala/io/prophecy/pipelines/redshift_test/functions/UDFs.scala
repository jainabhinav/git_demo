package io.prophecy.pipelines.redshift_test.functions

import _root_.io.prophecy.abinitio.ScalaFunctions._
import _root_.io.prophecy.libs._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql._

object UDFs extends Serializable {

  def registerUDFs(spark: SparkSession) = {
    spark.udf.register("rollup1615_UDF", rollup1615_UDF)
    spark.udf.register("rollup1621_UDF", rollup1621_UDF)
    spark.udf.register("rollup1627_UDF", rollup1627_UDF)
    registerAllUDFs(spark)
  }

  def rollup1615_UDF = {
    udf(
      (_view_result: Seq[Integer], _temp_view_result: Integer) => {
        var view_result      = _view_result
        var temp_view_result = _temp_view_result
        var i                = 0
        while (i < view_result.length) {
          temp_view_result =
            if (
              (if (temp_view_result == null)
                 0
               else
                 temp_view_result) == (if (view_result(i) == null)
                                         0
                                       else
                                         view_result(i)) || (if (
                                                               view_result(
                                                                 i
                                                               ) == null
                                                             )
                                                               0
                                                             else
                                                               view_result(
                                                                 i
                                                               )) == 0 || (if (
                                                                             temp_view_result == null
                                                                           )
                                                                             0
                                                                           else
                                                                             temp_view_result) == 1
            ) {
              if (temp_view_result == null)
                0
              else
                temp_view_result
            } else {
              if (
                (if (temp_view_result == null)
                   0
                 else
                   temp_view_result) == 0 || (if (view_result(i) == null)
                                                0
                                              else
                                                view_result(i)) == 1
              ) {
                if (view_result(i) == null)
                  0
                else
                  view_result(i)
              } else
                3
            }
          i = i + 1
        }
        temp_view_result
      },
      IntegerType
    )
  }

  def rollup1621_UDF = {
    udf(
      (
        _viewdef_definition_id:      Seq[Integer],
        _temp_viewdef_definition_id: Integer
      ) => {
        var viewdef_definition_id      = _viewdef_definition_id
        var temp_viewdef_definition_id = _temp_viewdef_definition_id
        var i                          = 0
        while (i < viewdef_definition_id.length) {
          temp_viewdef_definition_id =
            if (
              compareTo(if (temp_viewdef_definition_id == null)
                          0
                        else
                          temp_viewdef_definition_id,
                        if (viewdef_definition_id(i) == null)
                          0
                        else
                          viewdef_definition_id(i)
              ) > 0
            ) {
              if (temp_viewdef_definition_id == null)
                0
              else
                temp_viewdef_definition_id
            } else {
              if (
                compareTo(if (temp_viewdef_definition_id == null)
                            0
                          else
                            temp_viewdef_definition_id,
                          if (viewdef_definition_id(i) == null)
                            0
                          else
                            viewdef_definition_id(i)
                ) < 0
              ) {
                if (viewdef_definition_id(i) == null)
                  0
                else
                  viewdef_definition_id(i)
              } else {
                if (
                  compareTo(if (temp_viewdef_definition_id == null)
                              0
                            else
                              temp_viewdef_definition_id,
                            0
                  ) > 0
                ) {
                  if (temp_viewdef_definition_id == null)
                    0
                  else
                    temp_viewdef_definition_id
                } else
                  0
              }
            }
          i = i + 1
        }
        temp_viewdef_definition_id
      },
      IntegerType
    )
  }

  def rollup1627_UDF = {
    udf(
      (
        _viewdef_view_result:        Seq[Integer],
        _viewdef_definition_id:      Seq[Integer],
        _temp_viewdef_view_result:   Integer,
        _temp_viewdef_definition_id: Integer
      ) => {
        var viewdef_view_result        = _viewdef_view_result
        var viewdef_definition_id      = _viewdef_definition_id
        var temp_viewdef_definition_id = _temp_viewdef_definition_id
        var temp_viewdef_view_result   = _temp_viewdef_view_result
        var i                          = 0
        while (i < viewdef_view_result.length) {
          temp_viewdef_definition_id =
            if (
              compareTo(if (temp_viewdef_definition_id == null)
                          0
                        else
                          temp_viewdef_definition_id,
                        if (viewdef_definition_id(i) == null)
                          0
                        else
                          viewdef_definition_id(i)
              ) > 0
            ) {
              if (temp_viewdef_definition_id == null)
                0
              else
                temp_viewdef_definition_id
            } else {
              if (
                compareTo(if (temp_viewdef_definition_id == null)
                            0
                          else
                            temp_viewdef_definition_id,
                          if (viewdef_definition_id(i) == null)
                            0
                          else
                            viewdef_definition_id(i)
                ) < 0
              ) {
                if (viewdef_definition_id(i) == null)
                  0
                else
                  viewdef_definition_id(i)
              } else {
                if (
                  compareTo(if (temp_viewdef_definition_id == null)
                              0
                            else
                              temp_viewdef_definition_id,
                            0
                  ) > 0
                ) {
                  if (temp_viewdef_definition_id == null)
                    0
                  else
                    temp_viewdef_definition_id
                } else
                  0
              }
            }
          temp_viewdef_view_result =
            if (
              compareTo(if (temp_viewdef_definition_id == null)
                          0
                        else
                          temp_viewdef_definition_id,
                        if (viewdef_definition_id(i) == null)
                          0
                        else
                          viewdef_definition_id(i)
              ) > 0
            ) {
              if (temp_viewdef_view_result == null)
                0
              else
                temp_viewdef_view_result
            } else {
              if (
                compareTo(if (temp_viewdef_definition_id == null)
                            0
                          else
                            temp_viewdef_definition_id,
                          if (viewdef_definition_id(i) == null)
                            0
                          else
                            viewdef_definition_id(i)
                ) < 0
              ) {
                if (viewdef_view_result(i) == null)
                  0
                else
                  viewdef_view_result(i)
              } else {
                if (
                  compareTo(if (temp_viewdef_definition_id == null)
                              0
                            else
                              temp_viewdef_definition_id,
                            0
                  ) > 0
                ) {
                  if (
                    (if (temp_viewdef_view_result == null)
                       0
                     else
                       temp_viewdef_view_result) == (if (
                                                       viewdef_view_result(
                                                         i
                                                       ) == null
                                                     )
                                                       0
                                                     else
                                                       viewdef_view_result(
                                                         i
                                                       )) || (if (
                                                                viewdef_view_result(
                                                                  i
                                                                ) == null
                                                              )
                                                                0
                                                              else
                                                                viewdef_view_result(
                                                                  i
                                                                )) == 0 || (if (
                                                                              temp_viewdef_view_result == null
                                                                            )
                                                                              0
                                                                            else
                                                                              temp_viewdef_view_result) == 1
                  ) {
                    if (temp_viewdef_view_result == null)
                      0
                    else
                      temp_viewdef_view_result
                  } else {
                    if (
                      (if (temp_viewdef_view_result == null)
                         0
                       else
                         temp_viewdef_view_result) == 0 || (if (
                                                              viewdef_view_result(
                                                                i
                                                              ) == null
                                                            )
                                                              0
                                                            else
                                                              viewdef_view_result(
                                                                i
                                                              )) == 1
                    ) {
                      if (viewdef_view_result(i) == null)
                        0
                      else
                        viewdef_view_result(i)
                    } else
                      3
                  }
                } else
                  0
              }
            }
          i = i + 1
        }
        temp_viewdef_view_result
      },
      IntegerType
    )
  }

}

object PipelineInitCode extends Serializable
