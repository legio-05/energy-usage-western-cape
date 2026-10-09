"""
03 - PySpark analysis.

Reads readings_clean from MongoDB and runs the ten analytical queries
defined in the project plan.  Each query writes a single CSV part-file
to output/aggregates/<query_name>/ for downstream consumption by Tableau.

   Q1   Annual energy total per school
   Q2   Aggregate consumption per quintile (n, total, mean, median)
   Q3   Average load profile by school_type x weekend flag x hour
   Q4   Weekend vs weekday avg per school + per-quintile rollup
   Q5   Seasonal totals per quintile (Southern-hemisphere seasons)
   Q6   Term-day vs holiday/weekend mean per school + quintile
   Q7   Top-N peak intervals per school
   Q8   Overnight (01:00-04:30) baseload per school
   Q9   Top-N annual consumers
   Q10  Intervention impact (mean annual kWh by intervention status,
        also broken down per quintile)
"""
from pyspark.sql import SparkSession, functions as F, Window

from config import (
    MONGO_URI, COL_CLEAN, AGG_DIR, SPARK_PACKAGES,
    ANALYSIS_YEAR, NIGHT_HOURS, TOP_N_PEAKS, TOP_N_CONSUMERS,
)


def make_spark() -> SparkSession:
    return (SparkSession.builder
            .appName("WCED-Schools-Analysis")
            .config("spark.jars.packages", SPARK_PACKAGES)
            .config("spark.mongodb.read.connection.uri", MONGO_URI)
            .config("spark.sql.session.timeZone", "Africa/Johannesburg")
            .config("spark.driver.memory", "2g")  # <--- ADD THIS LINE HERE
            .getOrCreate())


def write_csv(df, name: str):
    """One coalesced CSV per query - small enough that a single partition is fine."""
    target = str(AGG_DIR / name)
    (df.coalesce(1).write.mode("overwrite")
       .option("header", True).csv(target))
    print(f"  -> {name}")


# --------------------------------------------------------------------------- #
# Individual queries
# --------------------------------------------------------------------------- #
def q1_annual_per_school(df):
    """Total kWh per school for ANALYSIS_YEAR plus per-learner intensity."""
    return (df.groupBy("school_id", "quintile", "school_type", "learners")
              .agg(F.round(F.sum("kWh"), 2).alias("annual_kwh"))
              .withColumn("annual_mwh", F.round(F.col("annual_kwh") / 1000, 2))
              .withColumn("kwh_per_learner",
                          F.round(F.col("annual_kwh") / F.col("learners"), 2))
              .orderBy(F.col("annual_kwh").desc()))


def q2_quintile_summary(df):
    """Per-school annual total, then aggregate by quintile."""
    per_school = (df.groupBy("school_id", "quintile")
                    .agg(F.sum("kWh").alias("annual_kwh")))
    return (per_school.groupBy("quintile")
            .agg(
                F.countDistinct("school_id").alias("n_schools"),
                F.round(F.sum("annual_kwh") / 1000, 1).alias("total_mwh"),
                F.round(F.avg("annual_kwh"), 0).alias("mean_kwh"),
                F.round(F.expr("percentile_approx(annual_kwh, 0.5)"), 0).alias("median_kwh"),
            )
            .orderBy("quintile"))


def q3_hourly_profile(df):
    """Average half-hour kWh by (school_type, is_weekend, hour)."""
    return (df.groupBy("school_type", "is_weekend", "hour")
              .agg(F.round(F.avg("kWh"), 3).alias("avg_kwh"),
                   F.round(F.expr("percentile_approx(kWh, 0.95)"), 3).alias("p95_kwh"))
              .orderBy("school_type", "is_weekend", "hour"))


def q4_weekend_weekday(df):
    """Per school weekend vs weekday mean half-hour kWh, then per-quintile rollup."""
    per_school = (df.groupBy("school_id", "quintile", "is_weekend")
                    .agg(F.avg("kWh").alias("avg_kwh"))
                    .groupBy("school_id", "quintile")
                    .pivot("is_weekend", [False, True])
                    .agg(F.first("avg_kwh"))
                    .withColumnRenamed("false", "weekday_avg")
                    .withColumnRenamed("true",  "weekend_avg")
                    .withColumn("weekend_ratio",
                                F.round(F.col("weekend_avg") / F.col("weekday_avg"), 3)))
    quintile_roll = (per_school.groupBy("quintile")
                     .agg(
                         F.countDistinct("school_id").alias("n_schools"),
                         F.round(F.avg("weekday_avg"), 3).alias("avg_weekday_kwh"),
                         F.round(F.avg("weekend_avg"), 3).alias("avg_weekend_kwh"),
                         F.round(F.avg("weekend_ratio"), 3).alias("avg_weekend_ratio"),
                     ).orderBy("quintile"))
    return per_school, quintile_roll


def q5_seasonal(df):
    """Total + average kWh per (quintile, season).  Uses the column derived
       in 02_pyspark_preprocessing (Southern-hemisphere meteorological seasons)."""
    return (df.groupBy("quintile", "season")
              .agg(F.round(F.sum("kWh") / 1000, 1).alias("total_mwh"),
                   F.round(F.avg("kWh"), 3).alias("avg_kwh"))
              .orderBy("quintile", "season"))


def q6_term_vs_holiday(df):
    """Average half-hour kWh on school days vs everything-else for each quintile."""
    return (df.groupBy("quintile", "schoolDay")
              .agg(F.round(F.avg("kWh"), 3).alias("avg_kwh"))
              .orderBy("quintile", "schoolDay"))


def q7_peak_intervals(df):
    """Top-N largest single half-hour readings per school."""
    win = Window.partitionBy("school_id").orderBy(F.col("kWh").desc())
    ranked = df.withColumn("rk", F.row_number().over(win))
    return (ranked.filter(F.col("rk") <= TOP_N_PEAKS)
            .select("school_id", "quintile", "school_type",
                    "timestamp", "kWh", "season", "hour", "rk")
            .orderBy("school_id", "rk"))


def q8_baseload(df):
    """Per-school overnight (01:00 - 04:30) baseload statistics."""
    night = df.filter((F.col("hour") >= NIGHT_HOURS[0]) &
                      (F.col("hour") <= NIGHT_HOURS[1]))
    return (night.groupBy("school_id", "quintile")
                 .agg(
                     F.round(F.avg("kWh"), 3).alias("baseload_avg_kwh"),
                     F.round(F.expr("percentile_approx(kWh, 0.5)"), 3).alias("baseload_median_kwh"),
                     F.round(F.min("kWh"), 3).alias("baseload_min_kwh"),
                 )
                 .orderBy(F.col("baseload_avg_kwh").desc()))


def q9_top_consumers(df):
    """Top-N annual consumers - drives the "where to focus retrofits" chart."""
    return (df.groupBy("school_id", "school_type", "quintile", "learners")
              .agg(F.round(F.sum("kWh"), 2).alias("annual_kwh"))
              .withColumn("kwh_per_learner",
                          F.round(F.col("annual_kwh") / F.col("learners"), 2))
              .orderBy(F.col("annual_kwh").desc())
              .limit(TOP_N_CONSUMERS))


def q10_intervention_impact(df):
    """Compare schools that have had an energy-efficiency intervention against
       those that have not, both overall and within each quintile."""
    per_school = (df.groupBy("school_id", "quintile", "intervention")
                    .agg(F.sum("kWh").alias("annual_kwh")))

    overall = (per_school.groupBy("intervention")
               .agg(
                   F.countDistinct("school_id").alias("n_schools"),
                   F.round(F.avg("annual_kwh"), 0).alias("mean_annual_kwh"),
                   F.round(F.expr("percentile_approx(annual_kwh, 0.5)"), 0).alias("median_annual_kwh"),
               )
               .orderBy("intervention"))

    by_quintile = (per_school.groupBy("quintile", "intervention")
                   .agg(
                       F.countDistinct("school_id").alias("n_schools"),
                       F.round(F.avg("annual_kwh"), 0).alias("mean_annual_kwh"),
                   )
                   .orderBy("quintile", "intervention"))
    return overall, by_quintile


# --------------------------------------------------------------------------- #
def main():
    spark = make_spark()
    print(">>> reading readings_clean from MongoDB")
    df = (spark.read.format("mongodb")
              .option("collection", COL_CLEAN).load()
              .filter(F.col("year") == ANALYSIS_YEAR))
    df.cache()                                                # 10 queries reuse it
    n = df.count()
    print(f"   {n:,} half-hour intervals for {ANALYSIS_YEAR}")

    AGG_DIR.mkdir(parents=True, exist_ok=True)

    print(">>> running ten analytical queries")
    write_csv(q1_annual_per_school(df),     "q1_annual_per_school")
    write_csv(q2_quintile_summary(df),      "q2_quintile_summary")
    write_csv(q3_hourly_profile(df),        "q3_hourly_profile")

    q4_school, q4_quintile = q4_weekend_weekday(df)
    write_csv(q4_school,   "q4_weekend_weekday_per_school")
    write_csv(q4_quintile, "q4_weekend_weekday_per_quintile")

    write_csv(q5_seasonal(df),              "q5_seasonal")
    write_csv(q6_term_vs_holiday(df),       "q6_term_vs_holiday")
    write_csv(q7_peak_intervals(df),        "q7_peak_intervals")
    write_csv(q8_baseload(df),              "q8_baseload")
    write_csv(q9_top_consumers(df),         "q9_top10_consumers")

    q10_overall, q10_by_q = q10_intervention_impact(df)
    write_csv(q10_overall, "q10_intervention_overall")
    write_csv(q10_by_q,    "q10_intervention_by_quintile")

    spark.stop()
    print("All queries finished.  Results -> output/aggregates/")


if __name__ == "__main__":
    main()
