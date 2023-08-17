import logging
import os
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame


DATASET_ONE_COLUMNS = {"id", "email", "country"}
DATASET_TWO_COLUMNS = {"id", "btc_a", "cc_t"}

logger = logging.getLogger("codac")


def read_csv_file(path: str) -> DataFrame:
    logger.info(f"creating DataFrame from file {path}")
    df = SparkSession.getActiveSession().read.csv(path, header=True)
    logger.info("checking DataFrame format")
    try:
        assert set(DATASET_ONE_COLUMNS) <= set(df.columns)
        columns = DATASET_ONE_COLUMNS
    except AssertionError:
        try:
            assert set(DATASET_TWO_COLUMNS) <= set(df.columns)
            columns = DATASET_TWO_COLUMNS
        except AssertionError:
            logger.error(f"file {path} structure is invalid")
            logger.error("data processing failed, application terminated")
            raise ValueError(
                f"{', '.join(DATASET_ONE_COLUMNS)} or \
                {', '.join(DATASET_TWO_COLUMNS)} \
                columns are required"
                )
    logger.info("cleaning DataFrame")
    for column in set(df.columns) - columns:
        df = df.drop(column)
    return df


def write_csv_file(data: DataFrame) -> None:
    logger.info("writing data to file")
    data.toPandas().to_csv(os.path.join("client_data", "dataset.csv"), index=False)
    logger.info("file saved")
