import argparse
import logging
import os
from logging.handlers import TimedRotatingFileHandler
from pathlib import Path

from pyspark.sql import SparkSession

from libs.dataset_operations import *
from libs.io_operations import *


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


def process_data(client_data_path, financial_data_path, countries):
    """
    """
    logger.info("process data procedure started")
    spark = SparkSession.builder.master("local").appName("codac").getOrCreate()
    logger.info("reading data")
    client_df = read_csv_file(client_data_path)
    financial_df = read_csv_file(financial_data_path)
    assert client_df.columns != financial_df.columns, "Both source files seem to have similar content."
    logger.info("processing data")
    final_df = join_dataframes(client_df, financial_df, "id")
    final_df = filter_dataframe(final_df, "country", countries)
    final_df = delete_column(final_df, "country")
    final_df = rename_column(
        final_df,
        dict(
            id="client_identifier",
            btc_a="bitcoin_address",
            cc_t="credit_card_type",
        )
    )
    logger.info("data processed")
    logger.info("saving data")
    write_csv_file(final_df)
    spark.stop()
    logger.info("process data procedure procedure finished")


if __name__ == "__main__":
    logger.info("application started")
    parser = argparse.ArgumentParser(description='Codac assesment.')
    parser.add_argument("one", type=str, help="the first data file")
    parser.add_argument("two", type=str, help="the second data file")
    parser.add_argument(
        "countries",
        nargs='+',
        help="list of countries, use quotes for names with spaces"
    )
    args = parser.parse_args()
    process_data(args.one, args.two, args.countries)
    logger.info("application stopped")
