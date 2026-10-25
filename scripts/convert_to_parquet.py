import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

spark = SparkSession.builder.appName("ConversionSchema").getOrCreate()

business_schema = StructType([
    StructField("business_id", StringType(), True),      # True means it allows null values
    StructField("name", StringType(), True),
    StructField("address", StringType(), True),
    StructField("city", StringType(), True),
    StructField("state", StringType(), True),
    StructField("postal_code", StringType(), True),
    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True),
    StructField("stars", DoubleType(), True),
    StructField("review_count", IntegerType(), True),
    StructField("is_open", IntegerType(), True),
    StructField("attributes", StringType(), True),
    StructField("hours", StringType(), True),
    StructField("categories", StringType(), True)
])

review_schema = StructType([
    StructField("review_id", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("business_id", StringType(), True),
    StructField("stars", DoubleType(), True),
    StructField("useful", IntegerType(), True),
    StructField("funny", IntegerType(), True),
    StructField("cool", IntegerType(), True),
    StructField("text", StringType(), True),
    StructField("date", StringType(), True) # I want to extract the year from eg "date":"2018-07-07 22:09:11"
])

user_schema = StructType([
    StructField("user_id", StringType(), True),
    StructField("fans", IntegerType(), True),
    StructField("review_count", IntegerType(), True),
    StructField("elite", StringType(), True),
    StructField("average_stars", DoubleType(), True),
    StructField("yelping_since", StringType(), True)
])

df = spark.read.schema(business_schema).json(
    os.path.join(
        os.path.expanduser("~"),
        "adv_db",
        "raw",
        "yelp_json",
        "yelp_academic_dataset_business.json",
    )
)

df.write.mode("overwrite").parquet(
    os.path.join(
        os.path.expanduser("~"),
        "adv_db",
        "parquet",
        "yelp_academic_dataset_business.parquet",
    )
)

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

df.write.mode("overwrite").partitionBy("year").parquet(
    os.path.join(
        os.path.expanduser("~"),
        "adv_db",
        "parquet",
        "yelp_academic_dataset_review.parquet",
    )
)

df = spark.read.schema(user_schema).json(
    os.path.join(
        os.path.expanduser("~"),
        "adv_db",
        "raw",
        "yelp_json",
        "yelp_academic_dataset_user.json",
    )
)

df.write.mode("overwrite").parquet(
    os.path.join(
        os.path.expanduser("~"),
        "adv_db",
        "parquet",
        "yelp_academic_dataset_user.parquet",
    )
)