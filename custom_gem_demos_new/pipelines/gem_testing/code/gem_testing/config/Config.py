from prophecy.config import ConfigBase


class Config(ConfigBase):

    def __init__(self, var_name: str=None, **kwargs):
        self.spark = None
        self.update(var_name)

    def update(self, var_name: str="20", **kwargs):
        prophecy_spark = self.spark
        self.var_name = var_name
        pass
