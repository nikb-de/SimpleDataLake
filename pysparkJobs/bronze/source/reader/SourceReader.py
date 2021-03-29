from abc import ABC, abstractmethod
from typing import Dict, Any

from pyspark.sql import DataFrame

from pysparkJobs.bronze.source.reader.SourceDesc import SourceDesc


class SourceReader(ABC):

    @abstractmethod
    def read_source_entity_to_df(self, source_desc: SourceDesc) -> DataFrame:
        pass
