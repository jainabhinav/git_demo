from gem_testing.graph.Subgraph_1.config.Config import SubgraphConfig as Subgraph_1_Config
from prophecy.config import ConfigBase


class Config(ConfigBase):

    def __init__(self, Subgraph_1: dict=None, var_name: str=None, **kwargs):
        self.spark = None
        self.update(Subgraph_1, var_name)

    def update(self, Subgraph_1: dict={}, var_name: str="20", **kwargs):
        prophecy_spark = self.spark
        self.Subgraph_1 = self.get_config_object(
            prophecy_spark, 
            Subgraph_1_Config(prophecy_spark = prophecy_spark), 
            Subgraph_1, 
            Subgraph_1_Config
        )
        self.var_name = var_name
        pass
