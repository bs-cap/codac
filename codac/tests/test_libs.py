import os
import shutil
import unittest

from chispa.dataframe_comparer import assert_df_equality
from pyspark.sql import SparkSession

from libs.dataset_operations import rename_column, filter_dataframe, join_dataframes
from libs.io_operations import read_csv_file, write_csv_file


class Tests(unittest.TestCase):
    def setUp(self) -> None:
        self.spark = SparkSession.builder.master("local").appName("chispa").getOrCreate()
        self.test_data = [
            ("a", "1"),
            ("b", "2"),
            ("g", "3")
        ]
        self.test_df = self.spark.createDataFrame(self.test_data, schema=["name", "value"])

    def tearDown(self) -> None:
        self.spark.stop()
        test_file_path = os.path.join("tests", "test.csv")
        if os.path.exists(test_file_path):
            shutil.rmtree(test_file_path, ignore_errors=True)

    def test_column_rename(self):
        result_df = rename_column(self.test_df, {"name": "test_name"})
        self.assertEqual(result_df.columns, ["test_name"])

    def test_dataframe_filtering(self):
        expected_data = [
            ("a", "1"),
        ]
        expected_df = self.spark.createDataFrame(expected_data, schema=["name", "value"])
        result_df = filter_dataframe(self.test_df, "value", ["1"])
        assert_df_equality(expected_df, result_df)

    def test_dataframes_join(self):
        data = [
            ("a", "alfa"),
            ("b", "beta"),
            ("g", "gamma"),
        ]
        df = self.spark.createDataFrame(data, schema=["name", "greek"])
        expected_data = [
            ("a", "1", "alfa"),
            ("b", "2", "beta"),
            ("g", "3", "gamma"),
        ]
        expected_df = self.spark.createDataFrame(expected_data, schema=["name", "value", "greek"])
        result_df = join_dataframes(self.test_df, df, "name")
        assert_df_equality(expected_df, result_df, ignore_column_order=True)

    def test_file_read(self):
        df_from_file = read_csv_file(os.path.join("tests", "one.csv"), {"name", "value"}, self.spark)
        assert_df_equality(self.test_df, df_from_file)

    def test_file_save(self):
        write_csv_file(self.test_df, os.path.join("tests", "test.csv"))
        df_from_file = read_csv_file(os.path.join("tests", "test.csv"), {"name", "value"}, self.spark)
        assert_df_equality(self.test_df, df_from_file)


if __name__ == '__main__':
    unittest.main()
