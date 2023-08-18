import logging
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame


logger = logging.getLogger("codac")


def read_csv_file(path: str, columns: set) -> DataFrame:
    """
    ============================
    Read DataFrame from CSV file
    ============================
    Helper function. Saving data from CSV file and converts it to PySpark DataFrame if possible. 
    CSV file has to have given columns. Other columns are deleted during processing.

    Parameters
    ----------
    * path - full/relative path to file with filename
    * columns - list of columns which are required

    Returns
    -------
    PySpark DataFrame with data from file limited to given columns
    """
    logger.info(f"creating DataFrame from file {path}")
    df = SparkSession.getActiveSession().read.csv(path, header=True)
    logger.info("checking DataFrame format")
    try:
        assert columns <= set(df.columns)
    except AssertionError:
        logger.error(f"file {path} structure is invalid, {columns} columns are required")
        logger.error("data processing failed, application terminated")
    logger.info("cleaning DataFrame")
    for column in set(df.columns) - columns:
        df = df.drop(column)
    return df


def write_csv_file(data: DataFrame, path: str) -> None:
    """
    ===========================
    Write DataFrame to CSV file
    ===========================
    Helper function. Saving data from PySpark DataFrame to comma-separated CSV file. 
    Function uses Pandas as a middleman.

    Parameters
    ----------
    * data - PySpark DataFrame with data to be saved
    * path - full/relative path to file with filename, path is created if does not exist

    Returns
    -------
    Nothing
    """
    logger.info("writing data to file")
    output_path = Path(path)
    Path(output_path.parent).mkdir(parents=True, exist_ok=True)
    data.toPandas().to_csv(output_path, index=False)
    logger.info("file saved")
