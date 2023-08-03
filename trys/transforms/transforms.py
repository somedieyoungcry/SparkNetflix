import pyspark.sql.functions as f
from pyspark.sql import Window
from pyspark.sql import DataFrame


class Transformation:
    def __init__(self):
        pass

    @staticmethod
    def count_series_year(df: DataFrame) -> DataFrame:
        series_year = df.withColumn("year_added", f.year(f.to_date("date_added", "MMMM d, yyyy"))) \
            .groupBy("year_added") \
            .agg(f.count("*").alias("total_series_por_año")) \
            .orderBy("year_added")

        return series_year

    @staticmethod
    def count_movies_year(df: DataFrame) -> DataFrame:
        movies_year = df.withColumn("year_added", f.year(f.to_date("date_added", "MMMM d, yyyy"))) \
            .filter(df["type"] == "Movie") \
            .groupBy("year_added") \
            .agg(f.count("*").alias("total_peliculas_por_año")) \
            .orderBy("year_added")
        return movies_year

    @staticmethod
    def top10_duration(df: DataFrame, top=True) -> DataFrame:
        window_func = Window.orderBy(df["duration"].desc() if top else df["duration"].asc())
        top10 = df.withColumn("row_number", f.row_number().over(window_func))
        return top10.where(top10["row_number"] <= 10)

    @staticmethod
    def top10_rating(df: DataFrame) -> DataFrame:
        window_func = Window.orderBy(f.asc("rating"))

        top10 = df.withColumn("row_number", f.row_number().over(window_func))
        return