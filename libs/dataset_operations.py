from typing import List

from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import col


def filter_dataframe(df: DataFrame, column: str, values: List[str]) -> DataFrame:
    return df.filter(df[column].isin(values))


def rename_column(df: DataFrame, values: dict) -> DataFrame:
    return df.select([col(key).alias(value) for key, value in values.items()])


def delete_column(df: DataFrame, column_name: str) -> DataFrame:
    return df.drop(column_name)
