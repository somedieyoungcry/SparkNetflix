import unittest
from unittest import TestCase
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql import Row
from trys.transforms.transforms import Transformation
import pytest
import pyspark.sql.functions as f

t = Transformation()


class TestsTransformations(TestCase):

    def setUp(self):
        self.spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()

        data = [
            ("show_id1", "Movie", "Movie 1", "Country 1", "2021-01-01", 2020, "8.5", "120 min"),
            ("show_id2", "Movie", "Movie 2", "Country 2", "2021-02-01", 2019, "9.2", "90 min"),
            ("show_id3", "Movie", "Movie 3", "Country 3", "2021-03-01", 2021, "7.9", "105 min"),
        ]

        schema = StructType([
            StructField("show_id", StringType(), True),
            StructField("type", StringType(), True),
            StructField("title", StringType(), True),
            StructField("country", StringType(), True),
            StructField("date_added", StringType(), True),
            StructField("release_year", IntegerType(), True),
            StructField("rating", StringType(), True),
            StructField("duration", StringType(), True),
        ])

        self.input_df = self.spark.createDataFrame(data, schema)

    def tearDown(self):
        self.spark.stop()

    def test_top10_rating_success(self):
        result_df = Transformation.top10_rating(self.input_df)

        self.assertEqual(result_df.count(), 10)

        ratings = result_df.select("rating").collect()
        ratings_list = [float(r[0]) for r in ratings]
        self.assertEqual(ratings_list, sorted(ratings_list, reverse=True))

    def test_top10_rating_error(self):
        schema = StructType([
            StructField("show_id", StringType(), True),
            StructField("type", StringType(), True),
            StructField("title", StringType(), True),
            StructField("country", StringType(), True),
            StructField("date_added", StringType(), True),
            StructField("release_year", IntegerType(), True),
            StructField("duration", StringType(), True),
        ])
        input_df_error = self.spark.createDataFrame([], schema)

        with self.assertRaises(Exception) as context:
            Transformation.top10_rating(input_df_error)

        self.assertTrue("Column 'rating' not found" in str(context.exception))


if __name__ == '__main__':
    unittest.main()
