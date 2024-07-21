from prophecy.config import ConfigBase


class Config(ConfigBase):

    def __init__(self, api_input_path: str=None, coin_api_key: str=None, **kwargs):
        self.spark = None
        self.update(api_input_path, coin_api_key)

    def update(
            self,
            api_input_path: str="dbfs:/Prophecy/abhinav@simpledatalabs.com/",
            coin_api_key: str="{\"X-CoinAPI-Key\":\"AC878A71-0493-4883-AC6C-CAC126E84B3E\"}",
            **kwargs
    ):
        prophecy_spark = self.spark
        self.api_input_path = api_input_path
        self.coin_api_key = coin_api_key
        pass
