"""
EMR PySpark Job - Seller Scoring
NusaCommerce Analytics Platform

Calculates a composite score for each seller based on four dimensions:
  - Total GMV (last 90 days): weight 40%
  - Transaction completion rate: weight 30%
  - Average rating: weight 20%
  - GMV growth rate (current vs previous month): weight 10%

Each dimension is normalized to 0-1 scale using Min-Max scaling.
"""

import sys
import argparse
from datetime import timedelta
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--transactions_path", required=True)
    parser.add_argument("--sellers_path", required=True)
    parser.add_argument("--output_path", required=True)
    args = parser.parse_args()

    spark = SparkSession.builder.appName("SellerScoring").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    # ── Read Data ─────────────────────────────────────────────────────────────
    print(f"[SellerScoring] Reading transactions from {args.transactions_path}")
    try:
        txn = spark.read.parquet(args.transactions_path)
        txn_count = txn.count()
        print(f"[SellerScoring] Transactions count: {txn_count}")
    except Exception as e:
        print(f"[SellerScoring] FATAL: Cannot read transactions: {e}")
        print(f"[SellerScoring] Path: {args.transactions_path}")
        spark.stop()
        sys.exit(1)

    print(f"[SellerScoring] Reading sellers from {args.sellers_path}")
    try:
        sel = spark.read.parquet(args.sellers_path)
        print(f"[SellerScoring] Sellers count: {sel.count()}")
    except Exception as e:
        print(f"[SellerScoring] FATAL: Cannot read sellers: {e}")
        print(f"[SellerScoring] Path: {args.sellers_path}")
        spark.stop()
        sys.exit(1)

    # ── Reference date (max date in dataset) ──────────────────────────────────
    ref_date_row = txn.agg(F.max(F.col("order_date").cast("date"))).collect()[0][0]
    if ref_date_row is None:
        print("[SellerScoring] ERROR: No transactions found!")
        spark.stop()
        return

    ref_date = ref_date_row
    cutoff_90d = ref_date - timedelta(days=90)
    current_month_start = ref_date.replace(day=1)
    prev_month_end = current_month_start - timedelta(days=1)
    prev_month_start = prev_month_end.replace(day=1)

    print(f"[SellerScoring] ref_date={ref_date}, cutoff_90d={cutoff_90d}")
    print(f"[SellerScoring] current_month={current_month_start}, prev_month={prev_month_start}-{prev_month_end}")

    # ── Dimension 1: Total GMV (last 90 days, COMPLETED only) ─────────────────
    gmv_df = (txn
              .filter(F.col("status") == "COMPLETED")
              .filter(F.col("order_date").cast("date") >= F.lit(cutoff_90d))
              .groupBy("seller_id")
              .agg(F.sum(F.col("total_amount").cast("double")).alias("total_gmv")))

    # ── Dimension 2: Completion rate (all time) ───────────────────────────────
    completion_df = (txn
                     .groupBy("seller_id")
                     .agg(
                         F.count("*").alias("all_orders"),
                         F.sum(F.when(F.col("status") == "COMPLETED", 1).otherwise(0)).alias("completed_orders")
                     )
                     .withColumn("completion_rate",
                                 F.col("completed_orders").cast("double") / F.col("all_orders").cast("double"))
                     .select("seller_id", "completion_rate"))

    # ── Dimension 4: GMV growth rate (current month vs previous month) ────────
    gmv_current = (txn
                   .filter(F.col("status") == "COMPLETED")
                   .filter(F.col("order_date").cast("date") >= F.lit(current_month_start))
                   .groupBy("seller_id")
                   .agg(F.sum(F.col("total_amount").cast("double")).alias("gmv_current_month")))

    gmv_prev = (txn
                .filter(F.col("status") == "COMPLETED")
                .filter(
                    (F.col("order_date").cast("date") >= F.lit(prev_month_start)) &
                    (F.col("order_date").cast("date") <= F.lit(prev_month_end))
                )
                .groupBy("seller_id")
                .agg(F.sum(F.col("total_amount").cast("double")).alias("gmv_prev_month")))

    growth_df = (gmv_current
                 .join(gmv_prev, "seller_id", "left")
                 .fillna({"gmv_current_month": 0.0, "gmv_prev_month": 0.0})
                 .withColumn("growth_rate",
                             F.when(F.col("gmv_prev_month") > 0,
                                    (F.col("gmv_current_month") - F.col("gmv_prev_month")) / F.col("gmv_prev_month"))
                             .otherwise(F.lit(0.0)))
                 .withColumn("growth_rate",
                             F.greatest(F.lit(-1.0), F.least(F.lit(5.0), F.col("growth_rate"))))
                 .select("seller_id", "growth_rate"))

    # ── Join all dimensions ───────────────────────────────────────────────────
    joined = (sel
              .select("seller_id", "seller_name", "tier",
                      F.col("rating").cast("double").alias("rating"))
              .join(gmv_df, "seller_id", "left")
              .join(completion_df, "seller_id", "left")
              .join(growth_df, "seller_id", "left")
              .fillna({"total_gmv": 0.0, "completion_rate": 0.0, "rating": 1.0, "growth_rate": 0.0}))

    joined.cache()
    print(f"[SellerScoring] Joined sellers: {joined.count()}")

    # ── Min-Max Normalization (pure DataFrame, no MLlib) ──────────────────────
    # Get min/max for each dimension
    stats = joined.agg(
        F.min("total_gmv").alias("gmv_min"), F.max("total_gmv").alias("gmv_max"),
        F.min("completion_rate").alias("cr_min"), F.max("completion_rate").alias("cr_max"),
        F.min("rating").alias("rat_min"), F.max("rating").alias("rat_max"),
        F.min("growth_rate").alias("gr_min"), F.max("growth_rate").alias("gr_max")
    ).collect()[0]

    def safe_range(mn, mx):
        r = (mx or 0.0) - (mn or 0.0)
        return r if r != 0 else 1.0

    gmv_min, gmv_range = stats["gmv_min"] or 0.0, safe_range(stats["gmv_min"], stats["gmv_max"])
    cr_min, cr_range = stats["cr_min"] or 0.0, safe_range(stats["cr_min"], stats["cr_max"])
    rat_min, rat_range = stats["rat_min"] or 0.0, safe_range(stats["rat_min"], stats["rat_max"])
    gr_min, gr_range = stats["gr_min"] or 0.0, safe_range(stats["gr_min"], stats["gr_max"])

    print(f"[SellerScoring] GMV range: {gmv_min:.2f} - {gmv_min + gmv_range:.2f}")
    print(f"[SellerScoring] Completion range: {cr_min:.4f} - {cr_min + cr_range:.4f}")
    print(f"[SellerScoring] Rating range: {rat_min:.2f} - {rat_min + rat_range:.2f}")
    print(f"[SellerScoring] Growth range: {gr_min:.4f} - {gr_min + gr_range:.4f}")

    # Apply min-max scaling
    result = (joined
              .withColumn("gmv_score", (F.col("total_gmv") - F.lit(gmv_min)) / F.lit(gmv_range))
              .withColumn("completion_score", (F.col("completion_rate") - F.lit(cr_min)) / F.lit(cr_range))
              .withColumn("rating_score", (F.col("rating") - F.lit(rat_min)) / F.lit(rat_range))
              .withColumn("growth_score", (F.col("growth_rate") - F.lit(gr_min)) / F.lit(gr_range))
              # Composite score: GMV 40%, Completion 30%, Rating 20%, Growth 10%
              .withColumn("composite_score",
                          F.col("gmv_score") * 0.4 +
                          F.col("completion_score") * 0.3 +
                          F.col("rating_score") * 0.2 +
                          F.col("growth_score") * 0.1)
              # Segment labels
              .withColumn("segment_label",
                          F.when(F.col("composite_score") >= 0.75, "TOP")
                          .when(F.col("composite_score") >= 0.50, "GROWING")
                          .when(F.col("composite_score") >= 0.25, "STABLE")
                          .otherwise("AT_RISK"))
              .select(
                  "seller_id", "seller_name", "tier",
                  F.round("composite_score", 4).alias("composite_score"),
                  F.round("gmv_score", 4).alias("gmv_score"),
                  F.round("completion_score", 4).alias("completion_score"),
                  F.round("rating_score", 4).alias("rating_score"),
                  F.round("growth_score", 4).alias("growth_score"),
                  "segment_label"
              ))

    # ── Write Output ──────────────────────────────────────────────────────────
    result.coalesce(4).write.mode("overwrite").parquet(args.output_path)

    count = result.count()
    print(f"[SellerScoring] Written {count} records to {args.output_path}")
    print("[SellerScoring] Segment distribution:")
    result.groupBy("segment_label").count().orderBy("segment_label").show()

    spark.stop()


if __name__ == "__main__":
    main()
