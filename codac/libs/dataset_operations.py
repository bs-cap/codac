from typing import List

from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import col


def filter_dataframe(df: DataFrame, column: str, values: List[str]) -> DataFrame:
    """
    =======================
    Filter DataFrame's rows
    =======================
    Helper function. Filtering rows of PySpark DataFrame by content of given column.

    Parameters
    ----------
    * df - PySpark DataFrame
    * column - DataFrame column to use filter on
    * values - list of values to be preserved

    Returns
    -------
    DataFrame with rows which contain required values in given column
    """
    return df.filter(df[column].isin(values))


def rename_column(df: DataFrame, values: dict) -> DataFrame:
    """
    ==========================
    Rename DataFrame's columns
    ==========================
    Helper function. Renaming columns of PySpark DataFrame. Columns which are not renamed are deleted.

    Parameters
    ----------
    * df - PySpark DataFrame
    * values - dictionary with old and new column names

    Returns
    -------
    DataFrame with renamed columns
    """
    return df.select([col(key).alias(value) for key, value in values.items()])


def join_dataframes(left_df: DataFrame, right_df: DataFrame, column: str) -> DataFrame:
    """
    ===================
    Join two DataFrames
    ===================
    Helper function. Joining two PySpark DataFrames by given identificator (column).

    Parameters
    ----------
    * left_df - PySpark DataFrame
    * right_df - PySpark DataFrame to be joined to left_df
    * column - name of column to be used as identifier

    Returns
    -------
    DataFrame with combined data from two source DataFrames
    """
    return left_df.join(right_df, column)
