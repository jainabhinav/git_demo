from prophecy.config import ConfigBase


class Config(ConfigBase):

    def __init__(
            self,
            current_snapshot: str=None,
            count_threshold: str=None,
            null_fk_threshold: str=None,
            update_view: bool=None,
            db_name: str=None,
            **kwargs
    ):
        self.spark = None
        self.update(current_snapshot, count_threshold, null_fk_threshold, update_view, db_name)

    def update(
            self,
            current_snapshot: str="1",
            count_threshold: str="0",
            null_fk_threshold: str="0",
            update_view: bool=False,
            db_name: str="abhinav_demo",
            **kwargs
    ):
        prophecy_spark = self.spark
        self.current_snapshot = current_snapshot
        self.count_threshold = count_threshold
        self.null_fk_threshold = null_fk_threshold
        self.update_view = self.get_bool_value(update_view)
        self.db_name = db_name
        pass
