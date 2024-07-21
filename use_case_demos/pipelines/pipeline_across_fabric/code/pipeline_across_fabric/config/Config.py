from prophecy.config import ConfigBase


class Config(ConfigBase):

    def __init__(self, api_input_path: str=None, **kwargs):
        self.spark = None
        self.update(api_input_path)

    def update(self, api_input_path: str="default", **kwargs):
        prophecy_spark = self.spark
        self.api_input_path = api_input_path
        pass
