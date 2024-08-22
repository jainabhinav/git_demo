from prophecy.config import ConfigBase


class Config(ConfigBase):

    def __init__(self, jdbc_username: str=None, jdbc_password: str=None, **kwargs):
        self.spark = None
        self.update(jdbc_username, jdbc_password)

    def update(self, jdbc_username: str="postgres", jdbc_password: str="Pr0phecy!23", **kwargs):
        prophecy_spark = self.spark
        self.jdbc_username = jdbc_username
        self.jdbc_password = jdbc_password
        pass
