package io.prophecy.pipelines.dxf_framework.graph

import io.prophecy.libs._
import io.prophecy.pipelines.dxf_framework.config.Context
import io.prophecy.pipelines.dxf_framework.udfs.UDFs._
import io.prophecy.pipelines.dxf_framework.udfs.PipelineInitCode._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object sk_service_placeholder {
  def apply(context: Context, in0: DataFrame): DataFrame = {
    val spark = context.spark
    val Config = context.config
    import org.apache.commons.codec.binary.Hex
    import org.apache.hadoop.conf.Configuration
    import org.apache.hadoop.fs._
    import org.apache.hadoop.io.IOUtils
    import org.apache.http.client.methods.HttpPost
    import org.apache.http.entity.StringEntity
    import org.apache.http.impl.client.HttpClientBuilder
    import org.apache.http.util.EntityUtils
    import org.apache.spark.sql.functions._
    import org.apache.spark.storage.StorageLevel
    import org.json4s._
    import org.json4s.jackson.JsonMethods._
    import org.json4s.jackson.Serialization.write
    import pureconfig._
    import pureconfig.generic.auto._
    import java.io.{IOException, _}
    import java.security.MessageDigest
    import java.time._
    import java.time.format._
    import java.util
    import java.sql.DriverManager
    import java.util.Properties
    import javax.crypto.Cipher
    import javax.crypto.spec.{IvParameterSpec, SecretKeySpec}
    import scala.collection.mutable.ListBuffer
    import scala.language.implicitConversions
    import scala.util.Try
    import spark.implicits._
    import com.databricks.dbutils_v1.DBUtilsHolder.dbutils
    
    implicit val formats = DefaultFormats
    
    println("#####Step name: create sk and create placeholders#####")
    println(
      "step start time: " + Instant.now().atZone(ZoneId.of("America/Chicago"))
    )
    
    spark.conf.set("main_table_api_type", "API")
    spark.conf.set("main_table_max_sk", "0")
    spark.conf.set("main_table_new_sk_count", "0")
    spark.conf.set("main_table_table_id", "-1")
    spark.conf.set("main_table_max_sk_update_sql", "")
    val isPKSet = spark.conf.getAll.contains("primary_key")
    val prim_key_columns = if (isPKSet) {
      spark.conf
        .get("primary_key")
        .split(",")
        .map(x => x.trim())
    } else {
      Config.primary_key
        .split(",")
        .map(x => x.trim())
    }
    val isSKSet = spark.conf.getAll.contains("sk_service_col")
    val sk_service_col = if (isSKSet) {
      spark.conf
        .get("sk_service_col")
        .toLowerCase
        .trim
    } else {
      Config.sk_service_col.toLowerCase.trim
    }
    
    Config.sk_table_name_override = if (Config.sk_table_name_override != "None"){
      println("main table name override for sk service: " + Config.sk_table_name_override)
      Config.sk_table_name_override
    } else {
      Config.target_table
    }
    
    val main_table_name = Config.sk_table_name_override
    val sk_service_base_url = Config.sk_service_base_url.toLowerCase match {
        case "none"  => dbutils.secrets.get(scope = Config.api_scope, key = Config.sk_service_base_url_key)
        case _ => Config.sk_service_base_url
      }
    
    val out0 =
      if (
        Config.enable_sk_service != "false" && spark.conf.get(
          "new_data_flag"
        ) == "true"
      ) {
    
        val api_header_key =
          dbutils.secrets.get(scope = Config.api_scope, key = Config.api_key)
    
        val current_ts_val = to_timestamp(
          from_utc_timestamp(current_timestamp(), "America/Chicago").cast("string"),
          "yyyy-MM-dd HH:mm:ss"
        )
    
        val skip_placeholder_tables =
          Config.list_tables_skip_delta_synapse_write_common_dimension_placeholder
            .split(",")
            .map(x => x.trim())
    
        val run_id = spark.conf.get("run_id")
    
        var run_id_for_data = run_id
        if (Config.custom_run_id_suffix != "None") {
          run_id_for_data = run_id.substring(0, 8) + Config.custom_run_id_suffix
        }
    
        println("run id for data: " + run_id_for_data)
    
        val load_ready_insert_path =
          Config.load_ready_insert_path
        val baseFilePath =
          Config.single_load_ready_path
    
        val hadoopConfig = new Configuration()
        val hdfs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    
        case class PkSkGen(pks_sks: String, sk_value: String, p_flag: String)
    
        case class SKDef(
            tableName: String,
            skCol: String,
            nkCol: List[String]
        )
    
        case class SKService(
            sks: List[SKDef]
        )
    
        case class PkSKDef(
            fmt: String,
            src: String,
            pkCols: List[String],
            skCols: String,
            synapseTable: Option[String] = None,
            encryptCols: Option[List[String]] = None,
            decryptCols: Option[List[String]] = None,
            dropPartitionCols: Option[List[String]] = None,
            orderByDedup: Option[String]=None,
            defaultToDecimalType: Option[String]=None
        )
    
        type PkSKDefs = Map[String, PkSKDef]
    
        val pkSKDefConfig =
          ConfigSource.string(Config.pk_sk_info).loadOrThrow[PkSKDefs]
    
        def jsonStrToMap(jsonStr: String): Map[String, Any] = {
          parse(jsonStr).extract[Map[String, Any]]
        }
    
        def get_sk_values_response(
            pk_to_find: List[String],
            post_url: String
        ): String = {
          val req =
            Map(
              "client_id" -> Config.pipeline_name,
              "request_id" -> (Config.pipeline_name + "_" + java.time.LocalDateTime.now.toString),
              "pks" -> pk_to_find,
              "detailed" -> "true"
            )
          val reqAsJson = write(req)
    
          val post = new HttpPost(
            post_url
          )
    
          post.setHeader("Content-type", "application/json")
          post.setHeader("Ocp-Apim-Subscription-Key", api_header_key)
          post.setEntity(new StringEntity(reqAsJson, "UTF-8"))
    
          val response = (HttpClientBuilder.create().build()).execute(post)
    
          val status_code = response.getStatusLine().getStatusCode()
    
          if (status_code == 206) {
            println(
              "Partial results got returned. Please connect with admin and check SK service memory. It should be less than 70 percent."
            )
          }
    
          if (status_code == 429) {
            Thread.sleep(60000)
            get_sk_values_response(pk_to_find, post_url)
          }
    
          val entity = response.getEntity
    
          var response_str = EntityUtils.toString(entity, "UTF-8")
          response_str
        }
    
        def get_sk_values(response: String, post_url: String): List[PkSkGen] = {
    
          var a = ""
          var b = ""
          var c = ""
          var response_str = response
          var pk_sk_values = jsonStrToMap(response_str)
            .get("pks_sks")
            .get
            .asInstanceOf[Map[String, Any]]
            .map {
              case (k, v) => {
                a = k
                b = v
                  .asInstanceOf[Map[String, Any]]
                  .get("value")
                  .get
                  .asInstanceOf[scala.math.BigInt]
                  .toString
                c = v
                  .asInstanceOf[Map[String, Any]]
                  .get("generated")
                  .get
                  .asInstanceOf[Boolean]
                  .toString
                PkSkGen(a, b, c)
              }
            }
    
          pk_sk_values.toList
        }
    
        def writeTextFile(
            filePath: String,
            filename: String,
            s: String
        ): Unit = {
    
          val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
          val file = new Path(filePath + "/" + filename)
          val dataOutputStream: FSDataOutputStream = fs.create(file)
          val bw: BufferedWriter = new BufferedWriter(
            new OutputStreamWriter(dataOutputStream, "UTF-8")
          )
          bw.write(s)
          bw.close()
    
          val crcPath = new Path(filePath + "/." + filename + ".crc")
          if (Try(fs.exists(crcPath)).isSuccess) {
            fs.delete(crcPath, true)
          }
        }
    
        def copyMerge(
            srcFS: FileSystem,
            srcDir: String,
            dstFS: FileSystem,
            dstFile: Path,
            deleteSource: Boolean,
            conf: Configuration,
            lazy_write: Boolean
        ): Boolean = {
          if (dstFS.exists(dstFile))
            throw new IOException(s"Target $dstFile already exists")
    
          val new_path_var = if (lazy_write) {
            srcDir + "_lazy/"
          } else {
            srcDir
          }
          val new_path = new Path(new_path_var)
          // Source path is expected to be a directory:
          if (srcFS.getFileStatus(new_path).isDirectory()) {
    
            val outputFile = dstFS.create(dstFile)
            Try {
              srcFS
                .listStatus(new_path)
                .sortBy(_.getPath.getName)
                .filter(_.getPath.getName.endsWith(".parquet"))
                .collect {
                  case status if status.isFile() =>
                    val inputFile = srcFS.open(status.getPath())
                    Try(IOUtils.copyBytes(inputFile, outputFile, conf, false))
                    inputFile.close()
                }
            }
            outputFile.close()
    
            if (deleteSource) {
              if (lazy_write) {
                srcFS.delete(new Path(srcDir), true)
              }
              srcFS.delete(new_path, true)
            } else true
          } else false
        }
    
        var encrypt_key = "Secret not defined"
        var encrypt_iv = "Secret not defined"
        var decrypt_key = "Secret not defined"
        var decrypt_iv = "Secret not defined"
    
        try {
    
          // Encryption method for obfuscating PHI / PII fields defined in config encryptColumns
          import com.databricks.dbutils_v1.DBUtilsHolder.dbutils
    
          // loading db secrets
          encrypt_key = dbutils.secrets.get(
            scope = Config.encrypt_scope,
            key = Config.encrypt_EncKey
          )
    
          encrypt_iv = dbutils.secrets.get(
            scope = Config.encrypt_scope,
            key = Config.encrypt_InitVec
          )
    
          decrypt_key = dbutils.secrets.get(
            scope = Config.decrypt_scope,
            key = Config.decrypt_EncKey
          )
    
          decrypt_iv = dbutils.secrets.get(
            scope = Config.decrypt_scope,
            key = Config.decrypt_InitVec
          )
          println("Found secrets successfully for encrypt decrypt UDFs.")
        } catch {
          case e: Exception => {
            println(
              "Please define databricks secrets for encrypt_key, decrypt_key, envrypt_iv and decrypt_iv on your cluster to use encrypt decrypt functions as udf"
            )
          }
        }
    
        def encrypt(key: String, ivString: String, plainValue: String): String = {
          if (plainValue != null) {
            val cipher: Cipher = Cipher.getInstance("AES/OFB/PKCS5Padding")
            cipher.init(Cipher.ENCRYPT_MODE, keyToSpec(key), getIVSpec(ivString))
            var encrypted_str =
              Hex.encodeHexString(cipher.doFinal(plainValue.getBytes("UTF-8")))
            encrypted_str = encrypted_str
            return encrypted_str
          } else {
            null
          }
        }
    
        def decrypt(
            key: String,
            ivString: String,
            encryptedValue: String
        ): String = {
          if (encryptedValue != null) {
            val cipher: Cipher = Cipher.getInstance("AES/OFB/PKCS5Padding")
            cipher.init(Cipher.DECRYPT_MODE, keyToSpec(key), getIVSpec(ivString))
            new String(cipher.doFinal(Hex.decodeHex(encryptedValue.toCharArray())))
          } else {
            null
          }
        }
    
        def keyToSpec(key: String): SecretKeySpec = {
          var keyBytes: Array[Byte] = (key).getBytes("UTF-8")
          // val sha: MessageDigest = MessageDigest.getInstance("MD5")
          keyBytes = Hex.decodeHex(key)
          keyBytes = util.Arrays.copyOf(keyBytes, 16)
          new SecretKeySpec(keyBytes, "AES")
        }
    
        def getIVSpec(IVString: String) = {
          new IvParameterSpec(IVString.getBytes() ++ Array.fill[Byte](16-IVString.length)(0x00.toByte))
    
        }
    
        val encryptUDF = udf(encrypt _)
        val decryptUDF = udf(decrypt _)
        spark.udf.register("aes_encrypt_udf", encryptUDF)
        spark.udf.register("aes_decrypt_udf", decryptUDF)
    
        in0.persist(StorageLevel.DISK_ONLY).count()
        var res = in0
        var final_output = res
        var final_output_main_tbl = res
    
        var sksConfig =
          ConfigSource.string(Config.optional_sk_config).loadOrThrow[SKService]
    
        var maxCounter = Config.max_retries_tbl_sk_gen //Defaults to 1
        var mainTableBreakFlag = false
        var tblRetryCounter = 0
        val mainTableSkListBuffer = ListBuffer[SKDef]() 
        mainTableSkListBuffer ++= sksConfig.sks
    
        if (sk_service_col != "none") {
          for (i <- 0 until maxCounter){
            mainTableSkListBuffer += SKDef(
                  tableName = main_table_name,
                  skCol = sk_service_col,
                  nkCol = prim_key_columns.toList
                )
          }
          
        }
    
        sksConfig = SKService(mainTableSkListBuffer.toList)
    
    
        println("sk and placeholder generate process started")
        println(
          "start time: " + Instant.now().atZone(ZoneId.of("America/Chicago"))
        )
    
        def identifyLastOccurrences(arr: Array[String]): Map[String, Int] = {
          var lastOccurrenceIndices: Map[String, Int] = Map.empty
    
          arr.indices.reverse.foreach { i =>
            lastOccurrenceIndices.get(arr(i)) match {
              case None    => lastOccurrenceIndices += (arr(i) -> i)
              case Some(_) => // Do nothing, already encountered
            }
          }
    
          lastOccurrenceIndices
        }
    
        def convertDataFrameToMap(df: DataFrame): Map[String, Map[String, Any]] = {
          val columns = df.columns
          val keyColumn = columns.head
          val valueColumns = columns.tail
    
          df.rdd
            .map { row =>
              val key = row.getAs[String](keyColumn)
              val values =
                valueColumns.map(column => column -> row.getAs[Any](column)).toMap
              key.toUpperCase -> values
            }
            .collectAsMap()
            .toMap
        }
    
        // import scala.collection.parallel._
        // val skServiceParallel: ParSeq[SKDef] = sksConfig.sks.toSeq.par
        // val forkJoinPool =
        //   new java.util.concurrent.ForkJoinPool(1) // Config.sk_service_parallel_pool
        // skServiceParallel.tasksupport = new ForkJoinTaskSupport(forkJoinPool)
    
        // code to join and collect dataframe
    
        val create_sk_table_list = sksConfig.sks.map(x => x.tableName).toArray
    
        val lastOccurrencesMap = identifyLastOccurrences(create_sk_table_list)
    
    
        val metadata_db_name =
        dbutils.secrets.get(
          scope = Config.metadata_scope,
          key = Config.metadata_dbname_key
        )
    
        val metadata_url =
        dbutils.secrets.get(
          scope = Config.metadata_scope,
          key = Config.metadata_url_key
        )
    
        val metadata_user =
        dbutils.secrets.get(
          scope = Config.metadata_scope,
          key = Config.metadata_user_key
        )
    
        val metadata_password =
        dbutils.secrets.get(
          scope = Config.metadata_scope,
          key = Config.metadata_password_key
        )
    
        val fs_access_key =
          dbutils.secrets.get(
            scope = Config.synapse_scope,
            key = Config.synapse_fs_access_key
          )
    
        val synapse_fs_access_url =
          dbutils.secrets.get(
            scope = Config.synapse_scope,
            key = Config.synapse_fs_access_url_key
          )
    
        val synapse_temp_dir =
          dbutils.secrets.get(
            scope = Config.synapse_scope,
            key = Config.synapse_temp_dir_key
          )
    
        spark.conf.set(
          synapse_fs_access_url,
          fs_access_key
        )
    
        val tables_for_sk =
          "('" + create_sk_table_list
            .map(x => x.toUpperCase)
            .distinct
            .mkString("', '") + "')"
    
        val sk_metadata =
          if (Config.sk_service_logic_from_metadata_table == "true") {
    
            val metadata_query =
              s"select A.TableName, B.TableType, B.ApiEnabled_IND, B.SkEnabled_IND, A.TableDetail_Id from ${Config.sql_server_lookup_table_detail} A inner join ${Config.sql_server_sk_metadata_table} B on A.TableDetail_Id = B.TableDetail_Id where upper(A.TableName) in ${tables_for_sk}"
    
            println("metadata_query: " + metadata_query)
    
            val sk_metadata_df = spark.read
              .format("jdbc")
              .option(
                "url",
                "jdbc:sqlserver://" + metadata_url + ";database=" + metadata_db_name
              )
              .option("user", metadata_user)
              .option("password", metadata_password)
              .option(
                "query",
                metadata_query
              )
              .option("useAzureMSI", "true")
              .option("tempDir", synapse_temp_dir)
              .load()
            val temp_map = convertDataFrameToMap(sk_metadata_df)
            val diff_na = create_sk_table_list
              .map(x => x.toUpperCase)
              .toSet
              .diff(temp_map.keys.toSet)
            if (diff_na.size > 0) {
              throw new Exception(
                "Missing entries found in SK service metadata table. Missing tables: " + diff_na
              )
            }
            temp_map
          } else {
            Map.empty[String, Map[String, Any]]
          }
    
        sksConfig.sks.zipWithIndex.map { ele =>
          val x = ele._1
          if( x.tableName != main_table_name || !mainTableBreakFlag) {
          
    
          println(
            "sk generate process started: " + x.tableName + " for sk_col: " + x.skCol
          )
          println(
            "start time: " + Instant.now().atZone(ZoneId.of("America/Chicago"))
          )
    
          var enabled = true
          var api_type = "API"
          var sk_table_id = "-1"
          var new_data = true
    
          if (Config.sk_service_logic_from_metadata_table == "true") {
            // overwrite above 2 variables
            val temp_sk_metadata = sk_metadata.get(x.tableName.toUpperCase)
            enabled =
              if (temp_sk_metadata.get("SkEnabled_IND").toString == "Y") true
              else false
            api_type = if (temp_sk_metadata.get("ApiEnabled_IND").toString == "Y") {
              "API"
            } else {
              "NON-API"
            }
            sk_table_id = temp_sk_metadata.get("TableDetail_Id").toString
          }
    
          val lit_value =  if (Config.enable_negative_one_self_join_sk && x.tableName == main_table_name) {
            "-9191"
          } else if (x.tableName == main_table_name) {
            "-1"
          } else {
            "-9090"
          }
    
          println("lit_value: " + lit_value)
    
          val placeholder_table_config = if (x.tableName != main_table_name) {
            pkSKDefConfig.get(x.tableName).get
          } else {
            PkSKDef(
              "",
              "",
              prim_key_columns.toList,
              sk_service_col
            )
          }
          var placeholder_table = if (x.tableName != main_table_name) {
            if (placeholder_table_config.fmt == "csv") {
              spark.read
                .format("csv")
                .option("header", true)
                .option("sep", ",")
                .load(placeholder_table_config.src)
            } else if (placeholder_table_config.fmt == "parquet") {
              spark.read
                .format("parquet")
                .load(placeholder_table_config.src)
            } else {
              spark.read.table(placeholder_table_config.src)
            }
          } else {
            spark.createDataFrame(Seq(("1", "1"), ("2", "2")))
          }
          val helper_col_pf = x.tableName + "_pf_" + x.skCol
          val helper_col_sk = x.tableName + "_sk_" + x.skCol
          val helper_col_nk = x.tableName + "_nk_" + x.skCol
    
          var new_max_sk = 0.toLong
          var new_sks_to_generate = 0.toLong
          if(x.tableName == main_table_name && tblRetryCounter == 0) {
            final_output_main_tbl = final_output 
          }
          if (enabled) {
            val df_to_eval = if (Config.sk_placeholder_skip_final_override == "true") {
              if (x.tableName == main_table_name && tblRetryCounter != 0)
                final_output_main_tbl
              else final_output
            } else res
    
            val temp_df = df_to_eval.where(
                  x.skCol + " is null or " + x.skCol + " == '" + lit_value + "'"
                )
                .select(
                  concat_ws(
                    "~|~",
                    x.nkCol.map { x => expr(x) }: _*
                  ).alias("concat_pk")
                )
            //println("dummy lit_value: " + lit_value)
            //val temp_df_count = temp_df.count()
            //if (temp_df_count == 0) {
            if(Config.debug_flag) {
              println(s"#### For Placeholder Generation of ${x.tableName}")
              temp_df.show(truncate=false)
            }
            if (temp_df.take(1).isEmpty) {
              new_data = false
            }
            //println("dummy temp_df count: " + temp_df_count)
    
            val sk_output: Seq[(String, String, String)] =
              if (api_type == "API" && new_data) {
    
                println("Genrating sk via old api sk service")
    
                var pk_to_find =
                  temp_df.collect().map(x => x.getString(0)).toList.distinct
    
                val pk_to_find_length = pk_to_find.length
                println("number of new sk to generate: " + pk_to_find_length)
    
                if (pk_to_find_length > 0) {
                  val post_url =
                    sk_service_base_url + x.tableName + "/get_sks"
                  val pk_batches =
                    pk_to_find.toSet.grouped(Config.sk_service_batch_size).toList
    
                  import scala.collection.mutable.ListBuffer
    
                  var temp_list: ListBuffer[PkSkGen] = ListBuffer()
    
                  var while_count = 0
                  pk_batches.foreach { x =>
                    val sk_input = x.toList
                    var sk_response_output = (get_sk_values(
                      get_sk_values_response(sk_input, post_url),
                      post_url
                    ))
    
                    while (
                      sk_response_output.length != sk_input.length && while_count < 5
                    ) {
                      println(
                        "sk_response_output length: " + sk_response_output.length
                      )
                      println("sk_input length: " + sk_input.length)
                      Thread.sleep(61000)
                      while_count = while_count + 1
                      println("while_count: " + while_count)
                      sk_response_output = (get_sk_values(
                        get_sk_values_response(sk_input, post_url),
                        post_url
                      ))
                      if (while_count == 3) {
                        throw new Exception(
                          "Please check SK service. Input response size and output response size did not matched in the response after 3 retries with 60 sec gap. Please connect with admin and see memory usaage for redis cache and clear it in case it is more that 70 percent."
                        )
                      }
    
                    }
                    temp_list = temp_list ++ sk_response_output
    
                  }
                  val sk_seq = temp_list
                    .map(row_val =>
                      (row_val.pks_sks, row_val.sk_value, row_val.p_flag)
                    )
                    .toSeq
                  new_sks_to_generate = sk_seq.length.toLong
                  sk_seq
                } else {
                  println("no new sk to fetch from api service: " + x.tableName)
                  Seq(("", "", ""))
                }
              } else if (api_type == "NON-API" && new_data) {
    
                println("Generating sk via new non-api sk service.")
                var lock = true
                var retry_count = 1
                val rand = new scala.util.Random
                while (lock) {
                  try {
                    val tryDelay = (1 + rand.nextInt(10)) * 1000
                    Thread.sleep(tryDelay)
                    dbutils.fs.put(
                      Config.sk_lock_file_path + x.tableName + ".txt",
                      x.tableName
                    )
                    lock = false
                  } catch {
                    case e: Exception => {
                      println(
                        x.tableName + " is locked. Retry: " + retry_count.toString
                      )
    
                      val retryDelay = (10 + rand.nextInt(30)) * 1000
                      Thread.sleep(retryDelay)
                      retry_count = retry_count + 1
                      if (retry_count > Config.sk_lock_retry_count.toInt) {
                        throw new Exception(
                          "Retry count limit reached. Please check sk_lock_path on storage to see for any locks on table and remove it before re-running the process."
                        )
                      }
                    }
                  }
                }
                println("Applying lock on: " + x.tableName)
                try {
                  val pk_cols = placeholder_table_config.pkCols
    
                  var available_sks = Array[(String, String, String)]()
                  val sks_to_generate = if (x.tableName != main_table_name) {
                    val available_keys_df = temp_df
                      .alias("in0")
                      .join(
                        placeholder_table
                          .select(
                            concat_ws(
                              "~|~",
                              pk_cols.map { x => expr(x) }: _*
                            ).alias("concat_pk"),
                            col(placeholder_table_config.skCols).alias("sk")
                          )
                          .alias("in1"),
                        col("in0.concat_pk") === col("in1.concat_pk"),
                        "left"
                      )
                      .select("in0.concat_pk", "in1.sk")
                      .dropDuplicates(List("concat_pk"))
    
                    available_sks = available_keys_df
                      .where("sk is not null")
                      .collect()
                      .map(x => (x.getString(0), x.getLong(1).toString, "false"))
    
                    available_keys_df
                      .where("sk is null")
                      .collect()
                      .map(x => x.getString(0).toString)
                      .toList
                  } else {
                    temp_df
                      .collect()
                      .map(x => x.getString(0).toString)
                      .toList
                  }
    
                  val max_sk_query =
                    s"select MaxSk from ${Config.sql_server_max_sk_table} where TableDetail_ID = ${sk_table_id}"
    
                  var curr_max_sk =
                    if (
                      !(x.tableName == main_table_name && Config.temp_output_flag == "true" && Config.max_sk_counter_reset_for_main_table == "true")
                    ) {
                      try {
                        spark.read
                          .format("jdbc")
                          .option(
                            "url",
                            "jdbc:sqlserver://" + metadata_url + ";database=" + metadata_db_name
                          )
                          .option("user", metadata_user)
                          .option("password", metadata_password)
                          .option(
                            "query",
                            max_sk_query
                          )
                          .option("useAzureMSI", "true")
                          .option("tempDir", synapse_temp_dir)
                          .load()
                          .collect()(0)(0)
                          .toString
                          .toLong
                      } catch {
                        case e: Exception => {
                          println(
                            "Exception while reading existing max SK from table. Please make sure that max SK entry is present for:" + x.tableName
                          )
                          dbutils.fs.rm(
                            Config.sk_lock_file_path + x.tableName + ".txt"
                          )
                          throw e
                        }
                      }
                    } else {
                      0.toLong
                    }
    
                  val new_sks_length = sks_to_generate.length
    
                  println("new sks to generate: " + new_sks_length)
    
                  new_max_sk = curr_max_sk + new_sks_length
                  new_sks_to_generate = new_sks_length
                  println("new_max_sk: " + new_max_sk)
                  if (x.tableName == main_table_name) {
                    spark.conf.set("main_table_api_type", "NON-API")
                    spark.conf.set("main_table_max_sk", new_max_sk.toString)
                    spark.conf.set(
                      "main_table_new_sk_count",
                      new_sks_to_generate.toString
                    )
                    spark.conf.set("main_table_table_id", sk_table_id.toString)
                  }
    
                  val new_sks = sks_to_generate.zipWithIndex.map(pk_ele =>
                    (pk_ele._1, (pk_ele._2 + curr_max_sk + 1).toString, "true")
                  )
                  available_sks ++ new_sks
                } catch {
                  case e: Exception => {
                    println(
                      "Exception while trying to generate SK. Removing Lock for:" + x.tableName
                    )
    
                    dbutils.fs.rm(
                      Config.sk_lock_file_path + x.tableName + ".txt"
                    )
                    throw e
                    Seq(("", "", ""))
                  }
                }
    
                // set new_max
    
              } else {
                if (new_data) {
                  throw new Exception(
                    "Undefined api service call type defined for " + x.tableName + " :" + api_type
                  )
                }
    
                Seq(("", "", ""))
              }
            if (new_data && new_sks_to_generate > 0) {
              try {
                if(Config.debug_flag) {
                  println("################### sk_output DF ############################")
                  sk_output.toDF("concat_pk", "service_sk", "placeholder_flag").where("service_sk IS NULL").show(truncate=false)
                }
                final_output = final_output
                  .withColumn(
                    x.tableName + "_concat_pk",
                    concat_ws(
                      "~|~",
                      x.nkCol.map { nk_col => expr(nk_col) }: _*
                    )
                  )
                  .as("in0")
                  .join(
                    sk_output
                      .toDF("concat_pk", "service_sk", "placeholder_flag")
                      .as("in1"),
                    expr("in0." + x.tableName + "_concat_pk == in1.concat_pk"),
                    "left"
                  )
                  .withColumn(
                    x.skCol,
                    when(
                      (col(x.skCol) === lit(lit_value)) || col(x.skCol).isNull,
                      col("service_sk")
                    ).otherwise(col(x.skCol)).cast("long")
                  )
                  .withColumn(
                    helper_col_pf,
                    col("in1.placeholder_flag")
                  )
                  .withColumn(helper_col_sk, col("in1.service_sk"))
                  .withColumn(helper_col_nk, col("in1.concat_pk"))
                if(Config.debug_flag && Config.final_output_debug_flag) {
                  println("################### final_output DF ############################")
                  final_output.select("in0." + x.tableName + "_concat_pk", "in1.concat_pk", "service_sk", x.skCol).where(s"${x.skCol} IS NULL").show(truncate=false)
                }
                final_output = final_output
                  .selectExpr(
                    "in0.*",
                    x.skCol,
                    helper_col_pf,
                    helper_col_sk,
                    helper_col_nk
                  )
              } catch {
                case e: Exception => {
    
                  if (api_type == "NON-API") {
                    println("Exception while trying to join SK. Removing Lock.")
    
                    dbutils.fs.rm(
                      Config.sk_lock_file_path + x.tableName + ".txt"
                    )
                  }
                  throw e
                }
              }
            } else {
              println(
                "No new SK to generate."
              )
            }
          } else {
            println(
              "sk and placeholder generate process skipped due to disable in metadata table: " + x.tableName
            )
          }
    
          if(x.tableName == main_table_name && maxCounter > 1) {
            val mainTableNullSKCnt = final_output.where(s"${x.skCol} IS NULL").count() 
            if(mainTableNullSKCnt == 0) {
              mainTableBreakFlag = true
              println(s"No Null SKs found in final_output. Exiting loop")
              
            } else {
              tblRetryCounter = tblRetryCounter + 1 
              println(s"${mainTableNullSKCnt} Null SKs found in final_output. Regenerating now. Attempt #${tblRetryCounter}")
              // TODO: Put final_output in debug path -> logf
              if (api_type == "NON-API") {
                dbutils.fs.rm(
                  Config.sk_lock_file_path + x.tableName + ".txt"
                )
              }
              val retry_log_file_full_path = s"${Config.retry_log_file_folder}/${x.tableName}/sk_gen_retry_${Config.target_table}_${run_id}_retry_num_${tblRetryCounter}"
              println(s"Writing Started for Intermediate DF at location: ${retry_log_file_full_path} ")
              final_output.write.mode("overwrite").parquet(retry_log_file_full_path)
              println(s"Writing Ended for Intermediate DF at location: ${retry_log_file_full_path} ")
              if(tblRetryCounter >= maxCounter) {
                throw new Exception("NULL SKs getting Generated even after retrying. Please contact Prophecy for help on this issue.")
              }
            }
          }
    
          println("SK generation process ended")
          println(
            "end time: " + Instant.now().atZone(ZoneId.of("America/Chicago"))
          )
    
          val update_sql =
            s"update ${Config.sql_server_max_sk_table} set MaxSk = ${new_max_sk}, UpdatedDate = cast(sysdatetimeoffset() at time zone 'Central Standard Time' as datetime), UpdatedUid = '${Config.pipeline_name}'  where TableDetail_ID = ${sk_table_id}"
    
          var path =
            Config.load_ready_insert_path + "/" + x.tableName + "/ph_" + Config.target_table + "/insert_files/" + run_id
    
          var lazy_placeholder = false
          var lazy_write = false
          val lazy_table_count_for_placeholder =
            sksConfig.sks.filter(row => row.tableName == x.tableName).length
          if (lazy_table_count_for_placeholder > 1) {
            println("creating temporary placeholder file.")
            lazy_placeholder = true
            lazy_write = if (ele._2 == lastOccurrencesMap.get(x.tableName).get) {
              true
            } else {
              false
            }
          }
    
          if (
            enabled && (x.tableName != main_table_name) && (lazy_write || new_sks_to_generate > 0) && (Config.skip_placeholder_tables_delta_load_ready == "false")
          ) {
            println(
              "placeholder generate process started: " + x.tableName + " for sk_col: " + x.skCol
            )
            println(
              "start time: " + Instant.now().atZone(ZoneId.of("America/Chicago"))
            )
    
            
            try {
              var placeholder_df = if (lazy_write && (new_sks_to_generate == 0)){
                final_output
              } else {
                if (placeholder_table_config.orderByDedup != None){
                  import scala.collection.mutable.ListBuffer
                  println("Deduplicating placeholder records on:" + placeholder_table_config.orderByDedup.get)
                  var outputList = new ListBuffer[org.apache.spark.sql.Column]()
                  val order_by_rules = placeholder_table_config.orderByDedup.get.split(',').map(x => x.trim())
    
                  order_by_rules.foreach { rule =>
                    if (rule.toLowerCase().contains(" asc")) {
                      if (rule.toLowerCase().contains(" asc nulls")) {
                        if (rule.toLowerCase().contains("nulls first")) {
                          outputList += asc_nulls_first(rule.split(" ")(0).trim())
                        } else {
                          outputList += asc_nulls_last(rule.split(" ")(0).trim())
                        }
                      } else {
                        outputList += asc(rule.split(" ")(0).trim())
    
                      }
                    } else {
                      if (rule.toLowerCase().contains(" desc nulls")) {
                        if (rule.toLowerCase().contains("nulls first")) {
                          outputList += desc_nulls_first(rule.split(" ")(0).trim())
                        } else {
                          outputList += desc_nulls_last(rule.split(" ")(0).trim())
                        }
                      } else {
                        outputList += desc(rule.split(" ")(0).trim())
                      }
    
                    }
                  }
    
                  val window = Window
                    .partitionBy(col(helper_col_nk), col(helper_col_sk))
                    .orderBy(
                      outputList: _*
                    )
                  final_output
                    .withColumn("dedup_row_num", row_number().over(window))
                    .where("dedup_row_num == 1")
                    .drop("dedup_row_num")
                    .withColumn("pk_arr", split(col(helper_col_nk), "~\\|~"))
                    .withColumn("sk", col(helper_col_sk))
                } else {
                  println("Deduplicating placeholder records on PK.")
                  final_output
                    .where(helper_col_pf + " == 'true'")
                    .withColumn("pk_arr", split(col(helper_col_nk), "~\\|~"))
                    .withColumn("sk", col(helper_col_sk))
                    .dropDuplicates(List("pk_arr", "sk"))
                }
              }
    
              var placeholder_df_count = if (lazy_write && (new_sks_to_generate == 0)){
                0
              } else{
                placeholder_df.count
              }
       
              println("placeholder_df_count: " + placeholder_df_count.toString)
    
              var final_write_df = if (placeholder_df_count > 0) {
    
                val pk_col_list = placeholder_table_config.pkCols.zipWithIndex.map {
                  case (pk_x, i) => col("pk_arr").getItem(i).as(pk_x)
                }.toList
    
                var final_ph_output = placeholder_df
                  .select(
                    (placeholder_df.columns
                      .filter(x =>
                        !(placeholder_table_config.pkCols.contains(
                          x
                        )) && (x != placeholder_table_config.skCols)
                      )
                      .map(x => col(x)) ++ pk_col_list ++ List(
                      col("sk").as(placeholder_table_config.skCols)
                    )): _*
                  )
                  .withColumn(
                    "insert_ts",
                    current_ts_val
                  )
                  .withColumn(
                    "update_ts",
                    current_ts_val
                  )
                  .withColumn(
                    "insert_uid",
                    substring(concat(lit("ph_"), lit(Config.target_table)), 0, 20)
                  )
                  .withColumn("update_uid", lit(null).cast("string"))
                  .withColumn(
                    "run_id",
                    lit(run_id_for_data).cast("long")
                  )
                  .withColumn("rec_stat_cd", lit("3").cast("short"))
                  .withColumn("src_env_sk", col("src_env_sk").cast("int"))
    
                if (Config.run_id_from_filename == "true") {
                  final_ph_output = final_ph_output.withColumn(
                    "run_id",
                    col("file_name_timestamp").cast("long")
                  )
                }
    
                val audit_columns =
                  Config.audit_cols.split(",").map(x => x.trim()).toList
    
                val final_cols = final_ph_output.columns.toList
    
                var outputList = new ListBuffer[org.apache.spark.sql.Column]()
    
                placeholder_table = placeholder_table.filter(lit(false))
    
                placeholder_table.dtypes
                  .map { case (ph_x, ph_y) => (ph_x, ph_y.toString.toLowerCase()) }
                  .foreach { case (col_name, y) =>
                    if (audit_columns.contains(col_name)) {
                      outputList += expr(col_name).as(col_name)
                    } else if (final_cols.contains(col_name)) {
                      val expression = y match {
                        case value if value.startsWith("string") =>
                          "coalesce(" + col_name + ", '-')"
                        case value if value.startsWith("date") =>
                          "cast(" + col_name + " as date)"
                        case value if value.contains("time") =>
                          "cast(" + col_name + " as timestamp)"
                        case value 
                          if value.startsWith("decimal") &&
                            placeholder_table_config.defaultToDecimalType != None && placeholder_table_config.defaultToDecimalType.get == "true" =>
                            "cast(case when " + col_name + " is not null then format_number(cast(" + col_name + " as decimal(38,10)), '0.##########') else CAST(null as string) end as " + y.replace("type", "") + ")"
                        case value
                            if value.startsWith("decimal") || value.startsWith(
                              "double"
                            ) || value.startsWith("float") =>
                          "cast(case when " + col_name + " is not null then format_number(cast(" + col_name + " as decimal(38,10)), '0.##########') else CAST(null as string) end as double)"
                        case _ =>
                          "cast(case when " + col_name + " is not null then format_number(cast(" + col_name + " as decimal(38,10)), '0.##########') else CAST(null as string) end as " + y
                            .replace("type", "") + ")"
                      }
                      outputList += expr(expression).as(col_name)
                    } else if (col_name.endsWith("_sk")) {
                      outputList += expr("cast(-1 as long)").as(col_name)
                    } else {
                      val expression = y match {
                        case value if value.startsWith("string") =>
                          "'-'"
                        case value if value.startsWith("date") =>
                          "cast('1900-01-01' as date)"
                        case value if value.startsWith("timestamp") =>
                          "cast('1900-01-01 00:00:00' as timestamp)"
                        case value 
                            if value.startsWith("decimal") &&
                             placeholder_table_config.defaultToDecimalType != None && placeholder_table_config.defaultToDecimalType.get == "true" =>
                             "CAST('0' as "  + y.replace("type", "") + ")"
                        case value
                            if value.startsWith("decimal") || value.startsWith(
                              "double"
                            ) || value.startsWith("float") =>
                          "CAST('0' as double)"
                        case _ =>
                          "CAST('0' as " + y
                            .replace("type", "") + ")"
                      }
                      outputList += expr(expression).as(col_name)
                    }
                  }
    
                final_ph_output
                  .select(outputList: _*)
                  .select(placeholder_table.columns.map { x =>
                    if (x.endsWith("_sk") && x != "src_env_sk") {
                      when(col(x) === "-9090" || col(x) === "-9191", lit(-1).cast("long"))
                        .otherwise(col(x))
                        .as(x)
                    } else { col(x) }
                  }: _*)
              } else {
                spark.read.table(placeholder_table_config.src).where("1==2")
              }
    
              if (placeholder_table_config.encryptCols != None) {
                val encrypt_col_list = placeholder_table_config.encryptCols.get
                println("Encrypting columns: ", encrypt_col_list)
                val final_write_df_cols = final_write_df.columns
                final_write_df =
                  final_write_df.select(final_write_df_cols.map { x =>
                    if (encrypt_col_list.contains(x)) {
                      expr(
                        s"aes_encrypt_udf('$encrypt_key','$encrypt_iv'," + x + ")"
                      ).as(x)
                    } else {
                      col(x)
                    }
                  }: _*)
              }
    
              if ((!skip_placeholder_tables.contains(x.tableName))) {
    
                if (placeholder_df_count > 0) {
                  try {
                    final_write_df = final_write_df.persist(StorageLevel.DISK_ONLY)
                    final_write_df.write
                      .format("delta")
                      .mode("append")
                      .saveAsTable(placeholder_table_config.src)
                    println("delta table write completed: " + x.tableName)
                  } catch {
                    case e: Exception => {
                      println(
                        s"""Process failed while writing data to delta table: ${x.tableName}. Removing lock. Please fix the issue and re-run the process."""
                      )
                      if (api_type == "NON-API") {
                        dbutils.fs.rm(
                          Config.sk_lock_file_path + x.tableName + ".txt"
                        )
                      }
                      throw e
                    }
                  }
                } else {
                  println("final_write_df is empty. Skipping delta write.")
                }
              }
    
              if (
                Config.generate_load_ready_files == "true" && (lazy_write || placeholder_df_count > 0)
              ) {
    
                try {
                  if (placeholder_table_config.decryptCols != None) {
                    val decrypt_col_list = placeholder_table_config.decryptCols.get
                    println("Decrypting columns: ", decrypt_col_list)
                    val final_write_df_cols = final_write_df.columns
                    final_write_df =
                      final_write_df.select(final_write_df_cols.map { x =>
                        if (decrypt_col_list.contains(x)) {
                          expr(
                            s"aes_decrypt_udf('$decrypt_key','$decrypt_iv'," + x + ")"
                          )
                            .as(x)
                        } else {
                          col(x)
                        }
                      }: _*)
                  }
    
                  val write_mode = if (lazy_placeholder) {
                    "append"
                  } else {
                    "overwrite"
                  }
    
                  if (placeholder_table_config.dropPartitionCols != None) {
                    println(
                      "dropping partition cols: " + placeholder_table_config.dropPartitionCols
                    )
                    final_write_df
                      .drop(placeholder_table_config.dropPartitionCols.get: _*)
                      .repartition(1)
                      .write
                      .format("parquet")
                      .mode(write_mode)
                      .save(path)
                  } else {
                    final_write_df
                      .repartition(1)
                      .write
                      .format("parquet")
                      .mode(write_mode)
                      .save(path)
                  }
                  println("temp placeholder file created")
                } catch {
                  case e: Exception => {
                    println(
                      s"""Generation of temp load ready files failed. Please create single load ready files from delta table, create trigger file and update event log."""
                    )
    
                    if (api_type == "NON-API") {
                      println(
                        s"""Please update max sk manually and remove lock to proceed.
              To update metadata table run this query in metadata db: ${update_sql}
              Also insert and update load ready files needs to be generated manually for corresponding run.
              To remove lock for ${x.tableName}. Run below command in databricks: 
              dbutils.fs.rm(${Config.sk_lock_file_path}${x.tableName}.txt")
              """
                      )
                    }
    
                    // dbutils.fs.rm(Config.sk_lock_file_path + Config.target_table + ".txt")
                    throw e
                  }
                }
    
                if ((!lazy_placeholder) || (lazy_write)) {
    
                  var final_ph_write_df = final_write_df
                  if (lazy_write) {
                    try {
                      final_ph_write_df = spark.read
                        .parquet(path)
                        .dropDuplicates(placeholder_table_config.pkCols)
                      final_ph_write_df
                        .repartition(1)
                        .write
                        .format("parquet")
                        .mode("overwrite")
                        .save(path + "_lazy/")
                      println("lazy combined temp placeholder file created.")
                    } catch {
                      case e: Exception => {
                        println(
                          s"""Generation of lazy load ready files failed. Please create single load ready file from ${path}_lazy/ path, create trigger file and update event log."""
                        )
    
                        if (api_type == "NON-API") {
                          println(
                            s"""Please update max sk manually and remove lock to proceed.
              To update metadata table run this query in metadata db: ${update_sql}
              Also insert and update load ready files needs to be generated manually for corresponding run.
              To remove lock for ${x.tableName}. Run below command in databricks: 
              dbutils.fs.rm(${Config.sk_lock_file_path}${x.tableName}.txt")
              """
                          )
                        }
    
                        // dbutils.fs.rm(Config.sk_lock_file_path + Config.target_table + ".txt")
                        throw e
                      }
                    }
                  }
                  val row_count = final_ph_write_df.count()
                  val tableName = x.tableName
    
                  if (row_count == 0) {
                    println(
                      "Skipping generation of load ready files as no rows to write."
                    )
                  } else {
                        
                    val finalInsertPath =  if (Config.ht2_flag){
                      new Path(
                        baseFilePath + "/" + tableName + "/" + run_id.substring(
                          0,
                          4
                        ) + "/placeholder." + Config.pipeline_name.toLowerCase.replace("ht2_", "") + "." + tableName + ".insert.000." + run_id + ".parquet"
                      )
                      } else {
                        new Path(
                        baseFilePath + "/" + tableName + "/" + run_id.substring(
                          0,
                          4
                        ) + "/placeholder." + Config.pipeline_name + "." + tableName + ".insert.000." + run_id + ".parquet"
                      )
                      }
    
                    val tempInsertPath = path
    
                    try {
                      copyMerge(
                        hdfs,
                        tempInsertPath,
                        hdfs,
                        finalInsertPath,
                        true,
                        hadoopConfig,
                        lazy_write
                      )
                      println("single load ready insert file created.")
                    } catch {
                      case e: Exception => {
                        val new_tempInsertPath =
                          if (lazy_write) tempInsertPath
                          else tempInsertPath + "_lazy"
                        println(
                          s"""Generation of single load ready file failed. Please create single load ready file from ${new_tempInsertPath} path, create trigger file and update event log."""
                        )
    
                        if (api_type == "NON-API") {
                          println(
                            s"""Please update max sk manually and remove lock to proceed.
              To update metadata table run this query in metadata db: ${update_sql}
              Also insert and update load ready files needs to be generated manually for corresponding run.
              To remove lock for ${x.tableName}. Run below command in databricks: 
              dbutils.fs.rm(${Config.sk_lock_file_path}${x.tableName}.txt")
              """
                          )
                        }
    
                        // dbutils.fs.rm(Config.sk_lock_file_path + Config.target_table + ".txt")
                        throw e
                      }
                    }
    
                    // trigger file generation
    
                    val dataset_name =
                      Config.trigger_file_content_data_mart_prefix + "." + tableName
                    val transfer_type = "incr"
                    val trigger_path =
                      Config.single_load_ready_path + "/" + tableName + "/" + run_id
                        .substring(0, 4)
    
                    // <TABLE_NAME>.<RUN_ID>.trigger
                    val fileName = if (Config.ht2_flag){
                      "placeholder." + Config.pipeline_name.toLowerCase.replace("ht2_", "") + "." + tableName + "." + run_id + ".trigger"
                    }
                    else{
                      "placeholder." + Config.pipeline_name + "." + tableName + "." + run_id + ".trigger"
                    }
                    val file_date = java.time.LocalDate.now.toString
                    val ins_parquet_file_name = if (Config.ht2_flag){
                    "placeholder." + Config.pipeline_name.toLowerCase.replace("ht2_", "") + "." + tableName + ".insert.000." + run_id + ".parquet"
                    }
                    else {
                      "placeholder." + Config.pipeline_name + "." + tableName + ".insert.000." + run_id + ".parquet"
                    }
                    val current_time = Instant
                      .now()
                      .atZone(ZoneId.of("America/Chicago"))
    
                    val current_time_trigger_file = current_time
                      .format(DateTimeFormatter.ofPattern("yyyyMMdd HH:mm:ss"))
                      .toString
    
                    val current_time_event_log = current_time
                      .format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))
                      .toString
    
                    val trggr_data =
                      dataset_name + "|" + transfer_type + "|" + ins_parquet_file_name + "||" + row_count.toString + "|0|" + current_time_trigger_file
                    try {
                      writeTextFile(trigger_path, fileName, trggr_data)
                      println("trigger file created")
                    } catch {
                      case e: Exception => {
                        println(
                          s"""Generation of trigger file failed. Please create trigger file from ${finalInsertPath} path and update event log."""
                        )
    
                        if (api_type == "NON-API") {
                          println(
                            s"""Please update max sk manually and remove lock to proceed.
              To update metadata table run this query in metadata db: ${update_sql}
              Also insert and update load ready files needs to be generated manually for corresponding run.
              To remove lock for ${x.tableName}. Run below command in databricks: 
              dbutils.fs.rm(${Config.sk_lock_file_path}${x.tableName}.txt")
              """
                          )
                        }
    
                        // dbutils.fs.rm(Config.sk_lock_file_path + Config.target_table + ".txt")
                        throw e
                      }
                    }
    
                    if (!skip_placeholder_tables.contains(x.tableName)) {
                      // write data to event log
                      try {
                        val event_log_df = spark
                          .createDataFrame(
                            Seq(
                              (
                                Config.data_mart,
                                Config.pipeline_name,
                                tableName,
                                run_id
                                  .substring(0, 4) + "/" + ins_parquet_file_name,
                                current_time_event_log
                              )
                            )
                          )
                          .toDF(
                            "data_mart",
                            "pipeline_name",
                            "event_table",
                            "event_file",
                            "created_ts"
                          )
    
                        event_log_df.write
                          .format("delta")
                          .mode("append")
                          .partitionBy("event_table")
                          .saveAsTable(Config.event_log_table_name)
                        println("event log entry created")
                      } catch {
                        case e: Exception => {
                          println(
                            s"""Updating event log failed. Please create event log entry from ${finalInsertPath} path."""
                          )
    
                          if (api_type == "NON-API") {
                            println(
                              s"""Please update max sk manually and remove lock to proceed.
              To update metadata table run this query in metadata db: ${update_sql}
              Also insert and update load ready files needs to be generated manually for corresponding run.
              To remove lock for ${x.tableName}. Run below command in databricks: 
              dbutils.fs.rm(${Config.sk_lock_file_path}${x.tableName}.txt")
              """
                            )
                          }
    
                          // dbutils.fs.rm(Config.sk_lock_file_path + Config.target_table + ".txt")
                          throw e
                        }
                      }
                    }
                  }
    
                } else {
                  println(
                    "Lazy write for placeholder file as more SK for table: " + x.tableName + " needs to be generated in other iterations"
                  )
                }
    
              }
    
              println(
                "placeholder generate process ended: " + x.tableName + " for sk_col: " + x.skCol
              )
              println(
                "end time: " + Instant.now().atZone(ZoneId.of("America/Chicago"))
              )
    
            } catch {
              case e: Exception => {
                println(
                  s"""Process failed while creating placeholder files. Please create single placeholder file from delta, create trigger file and update event log."""
                )
    
                if (api_type == "NON-API") {
                  println(
                    s"""Please update max sk manually and remove lock to proceed.
              To update metadata table run this query in metadata db: ${update_sql}
              Also insert and update load ready files needs to be generated manually for corresponding run.
              To remove lock for ${x.tableName}. Run below command in databricks: 
              dbutils.fs.rm(${Config.sk_lock_file_path}${x.tableName}.txt")
              """
                  )
                }
    
                // dbutils.fs.rm(Config.sk_lock_file_path + Config.target_table + ".txt")
                throw e
              }
            }
    
          }
    
          if (x.tableName == main_table_name) {
            spark.conf.set("main_table_max_sk_update_sql", update_sql)
          }
          if (
            api_type == "NON-API" && new_sks_to_generate > 0 && (x.tableName != main_table_name)
          ) {
            // update max_sk
    
            val jdbcUrl =
              s"jdbc:sqlserver://${metadata_url}:1433;database=${metadata_db_name}"
    
            // Create a Properties() object to hold the parameters.
            val connectionProperties = new Properties()
            connectionProperties.put("user", s"${metadata_user}")
            connectionProperties.put("password", s"${metadata_password}")
    
            val driverClass = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
            connectionProperties.setProperty("Driver", driverClass)
    
            val con =
              DriverManager.getConnection(jdbcUrl, connectionProperties)
            val stmt = con.createStatement()
    
            println("dummy update_sql: " + update_sql)
            try {
              stmt.execute(update_sql)
              println(
                "update max sk successful: " + x.tableName + " with value: " + new_max_sk
              )
            } catch {
              case e: Exception => {
                println(
                  s"""Updating max sk in metadata table failed. Please update max sk manually and remove lock to proceed.
              To update metadata table run this query in metadata db: ${update_sql}
              Also insert and update load ready files needs to be generated manually for corresponding run.
              To remove lock for ${x.tableName}. Run below command in databricks: 
              dbutils.fs.rm(${Config.sk_lock_file_path}${x.tableName}.txt")
              """
                )
    
                // dbutils.fs.rm(Config.sk_lock_file_path + Config.target_table + ".txt")
                throw e
              }
            }
          }
        if (api_type == "NON-API" && x.tableName != main_table_name) {
          println("Successful. Removing Lock.")
          dbutils.fs.rm(
            Config.sk_lock_file_path + x.tableName + ".txt"
          )
        }
        
        }
        }
        println("SK and placeholder generate process ended")
        println(
          "end time: " + Instant.now().atZone(ZoneId.of("America/Chicago"))
        )
        final_output
      } else {
        println(
          "sk and placeholder process skipped due to no new data or enable_sk_service flag being false."
        )
        in0
      }
    
    if(Config.debug_flag) {
      var printDf = out0
      if(Config.debug_filter.toLowerCase() != "none"){
        printDf = printDf.where(Config.debug_filter)
      }  
      if(Config.debug_col_list.toLowerCase() != "none"){
        val print_cols = Config.debug_col_list.split(",").map(x => x.trim())
        printDf.selectExpr(print_cols : _*).show(truncate=false)
      } else {
        printDf.show(truncate=true)
      }
    }
    out0
  }

}
