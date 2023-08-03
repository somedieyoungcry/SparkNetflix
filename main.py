from pyspark.sql import SparkSession
import pyspark.sql.functions as f
from trys.transforms.transforms import Transformation


def main():
    spark = SparkSession.builder.appName("spark_app")\
        .master("local[*]")\
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY")\
        .getOrCreate()
    complements_df = spark.read.parquet("resources/t_kdit_netflix_complement/t_kdit_netflix_complement.parquet")
    info_df = spark.read.parquet("resources/t_kdit_netflix_info/t_kdit_netflix_info.parquet")
    info_df.printSchema()
    complements_df.printSchema()
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
    top10_rating = t.top10_rating(info_df)
    top10_rating.show()

    print("Inciso 5")
    top_director = t.top_director(complements_df)
    top_director.show()

    print("Inciso 6")
    high_low_rank = t.high_low(complements_df)
    high_low_rank.show()

    print("Join 1.4 y 1.5")
    join_2_df = t.union_df(top10_rating, top_director)
    join_2_df.show()

    print("Inciso 7")
    add_column = t.add_date_diff(join_2_df)
    add_column.show()
    add_column.printSchema()


if __name__ == "__main__":
    main()
