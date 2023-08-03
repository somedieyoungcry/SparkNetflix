from pyspark.sql import SparkSession
import pyspark.sql.functions as f
from trys.transforms.transforms import Transformation


def main():
    spark = SparkSession.builder.appName("spark_app").master("local[*]").getOrCreate()
    complements_df = spark.read.parquet("resources/t_kdit_netflix_complement/t_kdit_netflix_complement.parquet")
    info_df = spark.read.parquet("resources/t_kdit_netflix_info/t_kdit_netflix_info.parquet")
    t = Transformation()

    print("Inciso 1")
    count_series = t.count_series_year(info_df)
    count_series.show()

    print("Inciso 2")
    count_movies = t.count_movies_year(info_df)
    count_movies.show()

    print("Inciso 3")
    # True devuelve el top 10 de las series con mayor duracion
    # False devuelve el top 10 de las series con menor duracion
    top10_durations = t.top10_duration(info_df, top=True)
    top10_durations.show()

    print("Inciso 4")

    print("Inciso 5")

    print("Inciso 6")

    print("Inciso 7")


if __name__ == "__main__":
    main()
