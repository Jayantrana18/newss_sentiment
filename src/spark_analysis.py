from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, count, lag, to_date, corr, regexp_extract
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType
from src.logger import logger
from src.config import SENTIMENT_OUTPUT_PATH, STOCK_PRICE_PATH

def run_spark_analysis():
    spark = SparkSession.builder \
        .appName("NewsSentimentAnalysis") \
        .master("local[*]") \
        .getOrCreate()

    # ===============================
    # SENTIMENT DATA
    # ===============================
    sentiment_schema = StructType([
        StructField("id", LongType(), True),
        StructField("headline", StringType(), True),
        StructField("url", StringType(), True),
        StructField("publisher", StringType(), True),
        StructField("date", StringType(), True),
        StructField("stock", StringType(), True),
        StructField("sentiment", StringType(), True),
        StructField("sentiment_score", DoubleType(), True),
    ])

    sentiment_df = spark.read.csv(
        SENTIMENT_OUTPUT_PATH,
        header=True,
        schema=sentiment_schema
    )

    sentiment_df = sentiment_df.filter(col("sentiment_score").isNotNull())

    # SAFE DATE PARSING
    from pyspark.sql.functions import when, length

    sentiment_df = sentiment_df.withColumn(
        "clean_date",
        regexp_extract(col("date"), r"\d{4}-\d{2}-\d{2}", 0)
    )

    sentiment_df = sentiment_df.withColumn(
    "date",
    when(length(col("clean_date")) == 0, None)
    .otherwise(to_date(col("clean_date"), "yyyy-MM-dd"))
    ).drop("clean_date")

    sentiment_df = sentiment_df.filter(col("date").isNotNull())

    daily_sentiment = sentiment_df.groupBy("date").agg(
        avg("sentiment_score").alias("daily_sentiment"),
        count("*").alias("news_volume")
    )

    # ===============================
    # PRICE DATA
    # ===============================
    price_schema = StructType([
        StructField("Date", StringType(), True),
        StructField("Open", DoubleType(), True),
        StructField("High", DoubleType(), True),
        StructField("Low", DoubleType(), True),
        StructField("Close", DoubleType(), True),
        StructField("Volume", LongType(), True),
    ])

    price_df = spark.read.csv(
        STOCK_PRICE_PATH,
        header=True,
        schema=price_schema
    )

    price_df = price_df.withColumn("date", to_date(col("Date"), "yyyy-MM-dd")) \
                       .filter(col("date").isNotNull())

    w = Window.orderBy("date")

    price_df = price_df.withColumn("prev_close", lag("Close").over(w))
    price_df = price_df.withColumn(
        "daily_return",
        (col("Close") - col("prev_close")) / col("prev_close")
    )

    # ===============================
    # JOIN
    # ===============================
    final_df = daily_sentiment.join(
        price_df.select("date", "daily_return"),
        on="date",
        how="inner"
    ).orderBy("date")

    final_df = final_df.withColumn(
        "next_day_return",
        lag("daily_return", -1).over(w)
    )

    final_df.show(10, truncate=False)

    final_df.select(
        corr("daily_sentiment", "daily_return")
        .alias("sentiment_return_corr")
    ).show()

    final_df.select(
        corr("daily_sentiment", "next_day_return")
        .alias("sentiment_next_day_return_corr")
    ).show()

    # ===============================
    # EXPORT (NO HADOOP)
    # ===============================
    final_df.toPandas().to_csv(
        "data/final_sentiment_price.csv",
        index=False
    )

    spark.stop()
    logger.info("Spark job completed successfully.")

if __name__ == "__main__":
    run_spark_analysis()
