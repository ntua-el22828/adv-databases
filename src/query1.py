import argparse
import os
import time
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

def main():
    parser = argparse.ArgumentParser(description="Run query1 DF version with cache or without.")
    parser.add_argument("--use_cache", action="store_true", help="Use to use cache before parts a and b.")
    parser.add_argument("--use_cache_table", action="store_true", help="Use to use cache on the whole table.")
    parser.add_argument("--pause", action="store_true", help="Set to 1 to pause before stoppring spark, in order to check cache.")
    args = parser.parse_args()

    spark = SparkSession.builder.appName("Query1_Schema_DF").getOrCreate()

    df = spark.read.parquet(
        os.path.join(
            os.path.expanduser("~"),
            "adv_db",
            "parquet",
            "yelp_parquet",
            "yelp_academic_dataset_review.parquet",
        )
    )

    if args.use_cache_table:
        df = df.cache()

    df = df.filter(F.col("useful") >= 1)
    df = df.filter(F.col("year").between(2015,2021))

    df = df.withColumn("text_length_cat", F.when(F.length(F.col("text")) < 250, "short").when(F.length(F.col("text")) < 750, "mid").when(F.length(F.col("text")) < 2000, "long").otherwise("vast"))
    df = df.select("review_id", "useful", "stars", "year", "text_length_cat")

    if args.use_cache:
        df = df.cache()

    # part a
    df1 = df.groupBy("text_length_cat").agg(
        F.count("review_id").alias("review_count"),
        F.avg("stars").alias("avg_stars"),
        F.avg("useful").alias("avg_useful")
    )
    df1 = df1.orderBy(F.desc("avg_useful"))

    time_start = time.perf_counter()

    result_a = df1.collect()

    time_end = time.perf_counter()

    print(f"TIME_ELAPSED_PART_A_DF = {time_end - time_start}")

    # part b
    df2 = df.groupBy("year", "text_length_cat").agg(F.count("*").alias("category_review_count"))
    df2 = df2.withColumn("year_review_count", F.sum("category_review_count").over(Window.partitionBy("year")))
    df2 = df2.withColumn("percentage", F.round( F.col("category_review_count") * 100 / F.col("year_review_count"), 2))
    df2 = df2.orderBy("year", "text_length_cat")

    time_start = time.perf_counter()

    result_b = df2.collect()

    time_end = time.perf_counter()

    print(f"TIME_ELAPSED_PART_B_DF = {time_end - time_start}")

    df1.explain(mode="formatted")
    df2.explain(mode="formatted")

    spark.pause()

    input()

if __name__ == "__main__":
    main()