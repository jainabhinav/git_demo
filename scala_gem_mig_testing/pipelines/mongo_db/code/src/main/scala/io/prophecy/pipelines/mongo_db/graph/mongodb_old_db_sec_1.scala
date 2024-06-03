package io.prophecy.pipelines.mongo_db.graph

import io.prophecy.libs._
import io.prophecy.pipelines.mongo_db.config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object mongodb_old_db_sec_1 {

  def apply(context: Context, df: DataFrame): Unit = {
    import com.databricks.dbutils_v1.DBUtilsHolder.dbutils
    df.write
      .format("mongodb")
      .mode("overwrite")
      .option(
        "connection.uri",
        ("xcv://" + (s"${dbutils.secrets
          .get(scope = "asd", key = "username")}") + ":" + (s"${dbutils.secrets
          .get(scope = "asd", key = "password")}") + "@" + "bnm").trim
      )
      .option("database",   "rty")
      .option("collection", "yui")
      .save()
  }

}
