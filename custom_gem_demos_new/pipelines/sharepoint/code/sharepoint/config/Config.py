from prophecy.config import ConfigBase


class Config(ConfigBase):

    def __init__(
            self,
            username: str=None,
            password: str=None,
            sftpusername: str=None,
            sftppassword: str=None,
            **kwargs
    ):
        self.spark = None
        self.update(username, password, sftpusername, sftppassword)

    def update(
            self,
            username: str="rakesh@prophecy340.onmicrosoft.com",
            password: str="Prophecy@2024",
            sftpusername: str="sftpuser",
            sftppassword: str="Prophecy@123",
            **kwargs
    ):
        prophecy_spark = self.spark
        self.username = username
        self.password = password
        self.sftpusername = sftpusername
        self.sftppassword = sftppassword
        pass
