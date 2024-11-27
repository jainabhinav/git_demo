from prophecy.config import ConfigBase


class Config(ConfigBase):

    def __init__(self, asd: str=None, **kwargs):
        self.spark = None
        self.update(asd)

    def update(self, asd: str="sd", **kwargs):
        prophecy_spark = self.spark
        self.asd = asd
        pass
