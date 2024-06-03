package io.prophecy.pipelines.mongo_db.graph

import io.prophecy.libs._
import io.prophecy.pipelines.mongo_db.config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object mongodb_old_config {

  def apply(context: Context): DataFrame = {
    import com.databricks.dbutils_v1.DBUtilsHolder.dbutils
    context.spark.read
      .format("mongodb")
      .option("connection.uri",
              f"${"cvb"}://${context.config.abc}:${"xcv"}@${"ewrn"}".trim
      )
      .option("database",   "cnjmy")
      .option("collection", "cvnb")
      .load()
  }

}
