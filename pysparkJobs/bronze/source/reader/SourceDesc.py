from typing import Dict, Any


class SourceDesc:
    def __init__(self, source_type: str, entity_name: str, spark_additional_props: Dict[str, Any]) -> None:
        self.source_type = source_type
        self.entity_name = entity_name
        self.spark_additional_props = spark_additional_props
