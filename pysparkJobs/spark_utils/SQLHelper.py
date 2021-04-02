from pyspark.sql.types import StructField, IntegerType, BooleanType, StringType, FloatType, DateType, LongType


def gen_nvl_values(field: StructField, alias_name: str = '') -> str:
    nvl_val = ''
    if field.dataType == StringType():
        nvl_val = "''"
    elif field.dataType == BooleanType():
        nvl_val = "BOOLEAN(0)"
    elif field.dataType == IntegerType():
        nvl_val = "0"
    elif field.dataType == LongType():
        nvl_val = "LONG(0)"
    elif field.dataType == FloatType():
        nvl_val = "FLOAT(1.0)"
    elif field.dataType == DateType():
        nvl_val = "DATE('9999-12-32')"

    alias_str = alias_name + '.' if alias_name != '' else ''
    return "NVL(" + alias_str + field.name + ',' + nvl_val + ')'


def gen_nvl_comparison(field: StructField, source_alias_name: str = '', target_alias_name: str = '') -> str:
    return f"{gen_nvl_values(field, source_alias_name)} <> {gen_nvl_values(field, target_alias_name)}"