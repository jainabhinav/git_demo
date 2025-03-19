from prophecy.config import ConfigBase


class Config(ConfigBase):

    def __init__(self, date_filter: str=None, process_name: str=None, **kwargs):
        self.spark = None
        self.update(date_filter, process_name)

    def update(self, date_filter: str="2001-01-01", process_name: str="process1", **kwargs):
        prophecy_spark = self.spark
        self.date_filter = date_filter
        self.process_name = process_name
        pass
