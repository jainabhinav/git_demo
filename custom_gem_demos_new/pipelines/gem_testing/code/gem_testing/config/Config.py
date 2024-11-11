from prophecy.config import ConfigBase


class Config(ConfigBase):

    def __init__(self, var_name: str=None, secret_test: dict=None, **kwargs):
        self.spark = None
        self.update(var_name, secret_test)

    def update(
            self,
            var_name: str="20",
            secret_test: dict={"providerType" : "Databricks", "secretScope" : "abhinav_demo", "secretKey" : "kafka_api_key"},
            **kwargs
    ):
        prophecy_spark = self.spark
        self.var_name = var_name

        if secret_test is not None:
            self.secret_test = self.get_secret_config_object(
                prophecy_spark, 
                ConfigBase.SecretValue(prophecy_spark = prophecy_spark), 
                secret_test, 
                ConfigBase.SecretValue
            )

        pass
