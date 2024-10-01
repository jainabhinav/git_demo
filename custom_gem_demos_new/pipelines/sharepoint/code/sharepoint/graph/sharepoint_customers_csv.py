from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from sharepoint.config.ConfigStore import *
from sharepoint.functions import *

def sharepoint_customers_csv(spark: SparkSession) -> DataFrame:
    import paramiko
    import os

    def download_file_from_sftp(username, password, host, remote_file_path, local_file_path):
        ssh_client = paramiko.SSHClient()
        ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

        try:
            ssh_client.connect(host, username = username, password = password)
            sftp_client = ssh_client.open_sftp()

            try:
                ssh_client.open_sftp().get(remote_file_path, local_file_path)
            finally:
                ssh_client.open_sftp().close()
        finally:
            ssh_client.close()

    # local_dir = os.path.dirname(self.props.path)
    # if not os.path.exists(local_dir):
    #     os.makedirs(local_dir)
    download_file_from_sftp(
        username = f"{Config.sftpusername}",
        password = f"{Config.sftppassword}",
        host = "104.197.67.53",
        remote_file_path = "/uploads/customers.csv",
        local_file_path = os.path.join("/tmp", os.path.basename("/uploads/customers.csv"))
    )
    from pyspark.dbutils import DBUtils
    DBUtils(spark).fs.cp(
        "file://{}".format(os.path.join("/tmp", os.path.basename("/uploads/customers.csv"))),
        "dbfs:/sftp/sftpdemo/customers.csv"
    )
    print("[Ok] file has been uploaded to DBFS path: {0}".format("dbfs:/sftp/sftpdemo/customers.csv"))

    return spark.read\
        .option("header", True)\
        .option("inferSchema", True)\
        .option("sep", ",")\
        .csv("dbfs:/sftp/sftpdemo/customers.csv")
