from pyspark.sql import DataFrame
from pyspark.sql.functions import lit

ctl_loading_field_name: str = "ctl_loading"
ctl_loading_time_field_name: str = "ctl_loading_time"


class Metadata:
    def __init__(self, ctl_loading: int, ctl_loading_time: str):
        self.ctl_loading = ctl_loading
        self.ctl_loading_time = ctl_loading_time

    def add_metadata_fields_to_df(self, df: DataFrame) -> DataFrame:
        df = df \
            .withColumn(ctl_loading_field_name, lit(self.ctl_loading)) \
            .withColumn(ctl_loading_time_field_name, lit(self.ctl_loading_time))

        return df
