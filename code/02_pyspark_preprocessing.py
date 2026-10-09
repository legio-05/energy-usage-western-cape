"""
02 - PySpark preprocessing.

Reads readings_raw + schools_metadata from MongoDB, joins them, fills
missing values, derives calendar / school-day features, and writes the
result back into readings_clean.  Everything downstream reads from that
single collection.

Output schema (readings_clean):
    school_id, timestamp, kWh, quintile, school_type, learners, educators,
    intervention, fees, year, month, day, hour, dayofweek, is_weekend,
    season, term, schoolDay
"""
from pyspark.sql import SparkSession, functions as F, Window

from config import (
    MONGO_URI, COL_RAW, COL_META, COL_CLEAN, SPARK_PACKAGES,
)


def make_spark() -> SparkSession:
    """Configure a Spark session with the official Mongo connector loaded."""
    return (
        SparkSession.builder
        .appName("WCED-Schools-Preprocessing")
        .config("spark.jars.packages", SPARK_PACKAGES)
        .config("spark.mongodb.read.connection.uri", MONGO_URI)
        .config("spark.mongodb.write.connection.uri", MONGO_URI)
        .config("spark.sql.session.timeZone", "Africa/Johannesburg")
        .getOrCreate()
    )


def read_collection(spark: SparkSession, name: str):
    return (spark.read.format("mongodb")
            .option("collection", name).load())


def write_collection(df, name: str, mode: str = "overwrite"):
    (df.write.format("mongodb")
        .option("collection", name)
        .mode(mode).save())


def add_calendar_features(df):
    """Pure derivations from the timestamp - cheap, idempotent."""
    return (df
        .withColumn("year",       F.year("timestamp"))
        .withColumn("month",      F.month("timestamp"))
        .withColumn("day",        F.dayofmonth("timestamp"))
        .withColumn("hour",       F.hour("timestamp"))
        .withColumn("dayofweek",  F.dayofweek("timestamp"))           # Sun=1
        .withColumn("is_weekend", F.col("dayofweek").isin(1, 7))
    )


def add_southern_season(df):
    """Southern-hemisphere meteorological seasons:
       Summer = Dec/Jan/Feb, Autumn = Mar-May, Winter = Jun-Aug, Spring = Sep-Nov.
    """
    return df.withColumn("season",
        F.when(F.col("month").isin(12, 1, 2), F.lit("Summer"))
         .when(F.col("month").isin(3, 4, 5),  F.lit("Autumn"))
         .when(F.col("month").isin(6, 7, 8),  F.lit("Winter"))
         .otherwise(F.lit("Spring"))
    )


def add_term_and_schoolday(df):
    """Approximate Western Cape public-school terms for 2023.
       (Public DBE calendar; close enough for analytical aggregation.)
    """
    term_expr = (
        F.when((F.col("timestamp") >= F.lit("2023-01-18").cast("timestamp")) &
               (F.col("timestamp") <= F.lit("2023-03-31").cast("timestamp")),
               F.lit("Term 1"))
         .when((F.col("timestamp") >= F.lit("2023-04-12").cast("timestamp")) &
               (F.col("timestamp") <= F.lit("2023-06-23").cast("timestamp")),
               F.lit("Term 2"))
         .when((F.col("timestamp") >= F.lit("2023-07-17").cast("timestamp")) &
               (F.col("timestamp") <= F.lit("2023-09-29").cast("timestamp")),
               F.lit("Term 3"))
         .when((F.col("timestamp") >= F.lit("2023-10-09").cast("timestamp")) &
               (F.col("timestamp") <= F.lit("2023-12-13").cast("timestamp")),
               F.lit("Term 4"))
         .otherwise(F.lit("Holiday"))
    )
    return df.withColumn("term", term_expr) \
             .withColumn("schoolDay",
                 F.when((F.col("term") != "Holiday") & (~F.col("is_weekend")),
                        F.lit("yes")).otherwise(F.lit("no")))


def fill_missing(df):
    """The publishers shipped clean_data/ already gap-filled with the per-
       school median for short outages.  Replicate that with a window
       so we can run on raw_data/ end-to-end without that intermediate file.
    """
    win = Window.partitionBy("school_id")
    median_kwh = F.expr("percentile_approx(kWh, 0.5)")
    return df.withColumn(
        "kWh",
        F.when(F.col("kWh").isNull(), median_kwh.over(win))
         .otherwise(F.col("kWh"))
    )


def cap_outliers(df):
    """Defensive: clip at 99.9th percentile per school to dampen meter spikes
       (a couple of schools have one-off readings two orders of magnitude
       larger than the rest of their year).
    """
    win = Window.partitionBy("school_id")
    p999 = F.expr("percentile_approx(kWh, 0.999)")
    return df.withColumn("kWh",
        F.when(F.col("kWh") > p999.over(win), p999.over(win))
         .otherwise(F.col("kWh"))
    )


def main():
    spark = make_spark()
    print(">>> reading raw readings + metadata from MongoDB")
    raw = read_collection(spark, COL_RAW)
    meta = read_collection(spark, COL_META).select(
        "school_id", "quintile", "school_type", "learners", "educators",
        "intervention", "fees",
    )

    print(">>> joining and engineering features")
    enriched = (raw.join(meta, on="school_id", how="left")
                   .transform(fill_missing)
                   .transform(cap_outliers)
                   .transform(add_calendar_features)
                   .transform(add_southern_season)
                   .transform(add_term_and_schoolday))

    # Drop the Mongo-internal _id field if present (it confuses the writer)
    if "_id" in enriched.columns:
        enriched = enriched.drop("_id")

    print(">>> writing readings_clean")
    write_collection(enriched, COL_CLEAN)

    n = enriched.count()
    print(f"readings_clean: {n:,} documents")
    spark.stop()


if __name__ == "__main__":
    main()
