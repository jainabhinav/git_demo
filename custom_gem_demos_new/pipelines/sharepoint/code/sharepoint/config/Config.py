from sharepoint.graph.Subgraph_1.config.Config import SubgraphConfig as Subgraph_1_Config
from prophecy.config import ConfigBase


class Config(ConfigBase):

    def __init__(
            self,
            username: str=None,
            password: str=None,
            sftpusername: str=None,
            sftppassword: str=None,
            Subgraph_1: dict=None,
            **kwargs
    ):
        self.spark = None
        self.update(username, password, sftpusername, sftppassword, Subgraph_1)

    def update(
            self,
            username: str="rakesh@prophecy340.onmicrosoft.com",
            password: str="Prophecy@2024",
            sftpusername: str="sftpuser",
            sftppassword: str="Prophecy@123",
            Subgraph_1: dict={},
            **kwargs
    ):
        prophecy_spark = self.spark
        self.username = username
        self.password = password
        self.sftpusername = sftpusername
        self.sftppassword = sftppassword
        self.Subgraph_1 = self.get_config_object(
            prophecy_spark, 
            Subgraph_1_Config(prophecy_spark = prophecy_spark), 
            Subgraph_1, 
            Subgraph_1_Config
        )
        pass
