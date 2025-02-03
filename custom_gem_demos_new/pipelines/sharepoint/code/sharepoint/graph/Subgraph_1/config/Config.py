from prophecy.config import ConfigBase


class SubgraphConfig(ConfigBase):

    def __init__(
            self,
            prophecy_spark=None,
            username: str="rakesh@prophecy340.onmicrosoft.com",
            password: str="Prophecy@2024",
            sftpusername: str="sftpuser",
            sftppassword: str="Prophecy@123",
            **kwargs
    ):
        self.username = username
        self.password = password
        self.sftpusername = sftpusername
        self.sftppassword = sftppassword
        pass

    def update(self, updated_config):
        self.username = updated_config.username
        self.password = updated_config.password
        self.sftpusername = updated_config.sftpusername
        self.sftppassword = updated_config.sftppassword
        pass

Config = SubgraphConfig()
