import argparse
import logging
import os
from logging.handlers import TimedRotatingFileHandler
from pathlib import Path, PurePosixPath
from typing import List, Set

from pyspark.sql import SparkSession
from pyspark.sql.utils import AnalysisException

from libs.dataset_operations import filter_dataframe, join_dataframes, rename_column
from libs.io_operations import read_csv_file, write_csv_file


COLUMNS = dict(
    client_data={"id", "email", "country"},
    financial_data={"id", "btc_a", "cc_t"},
)
NEW_NAMES = dict(
        id="client_identifier",
        btc_a="bitcoin_address",
        cc_t="credit_card_type",
    )
logs_path = "logs"
Path(logs_path).mkdir(parents=True, exist_ok=True)
rotator = TimedRotatingFileHandler(
    os.path.join(logs_path, "codac.log"),
    when="H",
    interval=1,
    backupCount=5,
    encoding="utf-8",
    utc=True,
)
logging.basicConfig(
    format="%(asctime)s %(levelname)s: %(message)s",
    handlers=[rotator],
    level=logging.INFO)
logger = logging.getLogger("codac")


def process_data(
        first_file_path: str, 
        first_file_columns: Set[str],
        second_file_path: str, 
        second_file_columns: Set[str],
        join_on_column: str,
        filter_on_column: str,
        values: List[str], 
        new_column_names: dict,
        output_path: str,
        session: SparkSession,
        logger:logging.Logger=logging.getLogger("")
        ) -> None:
    """
    ==============
    Data processor
    ==============
    Main function. Processing source data from two files and saving it to one result file.

    Parameters
    ----------
    * first_file_path - full/relative path to the first CSV file
    * first_file_columns - set of column names to be retrieved from the first source file
    * second_file_path - full/relative path to the second CSV file
    * second_file_columns - set of column names to be retrieved from the second source file
    * join_on_column - name of the column to be used as identifier for joining data from both source files
    * filter_on_column - name of the column to filter values in
    * values - list of the values to be selected from DataFrame
    * new_column_names - dictionary with old and new column names
    * output_path - full/relative path with file name to save result file to
    * session - PySpark Session
    * logger - logger instance

    Returns
    -------
    Nothing
    """
    logger.info("process data procedure started")
    logger.info("reading data")
    first_df = read_csv_file(first_file_path, first_file_columns, session)
    second_df = read_csv_file(second_file_path, second_file_columns, session)
    logger.info("processing data")
    final_df = join_dataframes(first_df, second_df, join_on_column)
    final_df = filter_dataframe(final_df, filter_on_column, values)
    final_df = rename_column(final_df, new_column_names)
    logger.info("saving data")
    write_csv_file(final_df, output_path)
    logger.info("process data procedure finished")


if __name__ == "__main__":
    """
    ==============
    Script invoker
    ==============
    Used when script is called directly from command line. After gathering all necessary
    data scripts calls ``process_data`` function.

    Usage
    -----
    python codac.py [-h] client_data financial_data countries [countries ...]

    Positional arguments (required)
    -------------------------------
    * client_data - path to client data file
    * financial_data - path to financial data file
    * countries - list of countries, use quotes for names with spaces

    Optional arguments
    ------------------
    -h, --help - show help message and exit
    """
    logger.info("application started")
    parser = argparse.ArgumentParser(description='Codac assesment.')
    parser.add_argument("client_data", type=str, help="path to client data file")
    parser.add_argument("financial_data", type=str, help="path to financial data file")
    parser.add_argument(
        "countries",
        nargs='+',
        help="list of countries, use quotes for names with spaces"
    )
    args = parser.parse_args()
    spark = SparkSession.builder.remote("sc://localhost:15002").appName("codac").getOrCreate()
    try:
        process_data(
            args.client_data, 
            COLUMNS["client_data"], 
            args.financial_data, 
            COLUMNS["financial_data"], 
            "id",
            "country",
            args.countries, 
            NEW_NAMES,
            PurePosixPath('/').joinpath("client_data", "dataset.csv").as_posix(),
            spark,
            logger,
            )
    except AssertionError:
        print("Invalid input file format")
        print("Application failed")
    except AnalysisException:
        print("Input file does not exist")
        print("Application failed")
    else:
        print("Application finished successfully")
    finally:
        spark.stop()
    logger.info("application stopped")
