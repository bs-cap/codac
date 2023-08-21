import logging
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.utils import AnalysisException


logger = logging.getLogger("codac")


def read_csv_file(path: str, columns: set, session: SparkSession) -> DataFrame:
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
    * session - PySpark Session

    Returns
    -------
    PySpark DataFrame with data from file limited to given columns
    """
    logger.info(f"creating DataFrame from file {path}")
    try:
        df = session.read.csv(path, header=True)
        logger.info("checking DataFrame format")
        assert columns <= set(df.columns)
    except AnalysisException:
        logger.error(f"file {path} does not exist")
        logger.error("data processing failed, processing terminated")
        raise
    except AssertionError:
        logger.error(f"file {path} structure is invalid, {', '.join(columns)} columns are required")
        logger.error("data processing failed, processing terminated")
        raise
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
    data.write.option("header", True).mode("overwrite").csv(path)
    logger.info("file saved")
