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
        window_spec = Window.orderBy(f.desc("rating"))
        top_100_mayor_rating = df.filter(f.col("rating").isNotNull()) \
            .withColumn("row_number", f.row_number().over(window_spec))

        return top_100_mayor_rating.filter(top_100_mayor_rating["row_number"] <= 100)

    @staticmethod
    def top_director(df: DataFrame) -> DataFrame:
        movies_no_null = df.dropna(subset=["director"])
        window_spec = Window.partitionBy("director")
        movies_count = movies_no_null.withColumn("num_peliculas", f.count("*").over(window_spec))
        movies_range = movies_count\
            .withColumn("rango", f.rank().over(Window.orderBy(f.col("num_peliculas").desc())))

        return movies_range.filter(f.col("rango") == 1).drop("rango")

    @staticmethod
    def high_low(df: DataFrame) -> DataFrame:
        df_count = df.groupBy("listed_in").agg(f.count("*").alias("num_registros"))
        windows_high = Window.orderBy(f.col("num_registros").desc())
        windows_low = Window.orderBy(f.col("num_registros").asc())
        df_ranks = df_count.withColumn("top_high", f.rank().over(windows_high))\
            .withColumn("top_low", f.rank().over(windows_low))

        return df_ranks

    @staticmethod
    def union_df(df1: DataFrame, df2: DataFrame) -> DataFrame:
        joined_df = df1.join(df2, on="show_id", how="inner")
        final_df = joined_df.filter(joined_df["director"].isNotNull())

        return final_df

    @staticmethod
    def add_date_diff(df: DataFrame) -> DataFrame:
        return df.withColumn("date_diff",
                             f.datediff(f.to_date("date_added", "MMM d, yyyy"), f.to_date("release_year", "yyyy"))) \
            .drop("release_year")


