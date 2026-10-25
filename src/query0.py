import argparse
import os
import time
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

def main():
    parser = argparse.ArgumentParser(description="Run just 1 variant for query0.")
    parser.add_argument("variant", type=str, help="Select json-infer, json-schema or parquet.")
    parser.add_argument("--explain-only", action="store_true", help="Set to 1 to run only the explains for PushedFilters, PartitionFilters.")
    args = parser.parse_args()

    spark = SparkSession.builder.appName("Query0_Schema_" + args.variant).getOrCreate()

    review_schema = StructType([
        StructField("review_id", StringType(), True),
        StructField("user_id", StringType(), True),
        StructField("business_id", StringType(), True),
        StructField("stars", DoubleType(), True),
        StructField("useful", IntegerType(), True),
        StructField("funny", IntegerType(), True),
        StructField("cool", IntegerType(), True),
        StructField("text", StringType(), True),
        StructField("date", StringType(), True) 
    ])

    time_start = time.perf_counter()

    if args.variant == "json-infer":
        df = spark.read.json(
            os.path.join(
                os.path.expanduser("~"),
                "adv_db",
                "raw",
                "yelp_json",
                "yelp_academic_dataset_review.json",
            )
        )
        df = df.withColumn("year", F.year(F.to_date(df.date)))

    elif args.variant == "json-schema":
        df = spark.read.schema(review_schema).json(
            os.path.join(
                os.path.expanduser("~"),
                "adv_db",
                "raw",
                "yelp_json",
                "yelp_academic_dataset_review.json",
            )
        )
        df = df.withColumn("year", F.year(F.to_date(df.date)))

    elif args.variant == "parquet":
        df = spark.read.parquet(
            os.path.join(
                os.path.expanduser("~"),
                "adv_db",
                "parquet",
                "yelp_parquet",
                "yelp_academic_dataset_review.parquet",
            )
        )

    if args.explain_only:

        df = df.filter(F.col("year") >= 2015)
        df = df.filter(F.col("stars") >= 3.0)
        df = df.groupBy("year").agg(F.avg("stars").alias("avg_stars")).orderBy("year")
        df.explain(mode="formatted")
        spark.stop()
        exit(0)

    df = df.groupBy("year").agg(F.avg("stars").alias("avg_stars")).orderBy("year")

    result = df.collect()
        
    time_end = time.perf_counter()

    print(f"TIME_ELAPSED = {time_end - time_start}")

    for row in result:
        print(f"Year: {row.year}, Avg Stars: {row.avg_stars:.6f}")

    df.explain(mode="formatted")

    spark.stop()

if __name__ == "__main__":
    main()