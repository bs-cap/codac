import argparse
import logging
import os
from logging.handlers import TimedRotatingFileHandler
from pathlib import Path

from pyspark.sql import SparkSession

from libs.dataset_operations import *
from libs.io_operations import *


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
        first_file_columns: List[str],
        second_file_path: str, 
        second_file_columns: List[str],
        join_on_column: str,
        filter_on_column: str,
        countries: List[str], 
        new_column_names: dict,
        output_path: str,
        ) -> None:
    """
    """
    logger.info("process data procedure started")
    logger.info("reading data")
    first_df = read_csv_file(first_file_path, first_file_columns)
    second_df = read_csv_file(second_file_path, second_file_columns)
    logger.info("processing data")
    final_df = join_dataframes(first_df, second_df, join_on_column)
    final_df = filter_dataframe(final_df, filter_on_column, countries)
    final_df = rename_column(final_df, new_column_names)
    logger.info("saving data")
    write_csv_file(final_df, output_path)
    logger.info("process data procedure finished")


if __name__ == "__main__":
    logger.info("application started")
    parser = argparse.ArgumentParser(description='Codac assesment.')
    parser.add_argument("client_data", type=str, help="client data file")
    parser.add_argument("financial_data", type=str, help="financial data file")
    parser.add_argument(
        "countries",
        nargs='+',
        help="list of countries, use quotes for names with spaces"
    )
    args = parser.parse_args()
    spark = SparkSession.builder.master("local").appName("codac").getOrCreate()
    process_data(
        args.client_data, 
        COLUMNS["client_data"], 
        args.financial_data, 
        COLUMNS["financial_data"], 
        "id",
        "country",
        args.countries, 
        NEW_NAMES,
        os.path.join("client_data", "dataset.csv"),
        )
    spark.stop()
    logger.info("application stopped")
