import os
import shutil
import unittest

from chispa.dataframe_comparer import assert_df_equality
from pyspark.sql import SparkSession

from codac import process_data
from libs.io_operations import read_csv_file


class Tests(unittest.TestCase):
    def setUp(self) -> None:
        self.spark = SparkSession.builder.master("local").appName("chispa").getOrCreate()

    def tearDown(self) -> None:
        self.spark.stop()
        full_file_path = os.path.join("tests", "full.csv")
        if os.path.exists(full_file_path):
            shutil.rmtree(full_file_path, ignore_errors=True)

    def test_process_data(self):
        process_data(
            os.path.join("tests", "one.csv"),
            {"name", "greek"},
            os.path.join("tests", "two.csv"),
            {"name", "from"},
            "name",
            "from",
            ["mars"],
            {"name": "id", "from": "origin"},
            os.path.join("tests", "full.csv"), 
            self.spark,
        )
        expected_data = [
            ("a", "mars"),
            ("g", "mars"),
        ]
        expected_df = self.spark.createDataFrame(expected_data, schema=["id", "origin"])
        df_from_file = read_csv_file(os.path.join("tests", "full.csv"), {"id", "origin"}, self.spark)
        assert_df_equality(expected_df, df_from_file)


if __name__ == '__main__':
    unittest.main()
