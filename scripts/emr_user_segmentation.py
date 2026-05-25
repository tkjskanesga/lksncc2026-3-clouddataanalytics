"""
EMR PySpark Job - User Segmentation (RFM + Quantile-based Clustering)
NusaCommerce Analytics Platform

Builds an RFM (Recency, Frequency, Monetary) model per user,
normalizes using z-score, then segments into 4 clusters using
quantile-based scoring to simulate K-Means behavior.

Segment labels assigned based on RFM characteristics:
  CHAMPION: low recency, high frequency, high monetary
  LOYAL: moderate recency, high frequency
  AT_RISK: high recency, low frequency
  DORMANT: highest recency, lowest frequency & monetary
"""

import sys
import argparse
from datetime import timedelta
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--transactions_path", required=True)
    parser.add_argument("--user_events_path", required=False, default="")
    parser.add_argument("--output_path", required=True)
    args = parser.parse_args()

    spark = SparkSession.builder.appName("UserSegmentation").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    # ── Read Data ─────────────────────────────────────────────────────────────
    print(f"[UserSegmentation] Reading transactions from {args.transactions_path}")
    try:
        txn = spark.read.parquet(args.transactions_path)
        print(f"[UserSegmentation] Transactions count: {txn.count()}")
    except Exception as e:
        print(f"[UserSegmentation] FATAL: Cannot read transactions: {e}")
        spark.stop()
        sys.exit(1)

    # ── Build RFM Features ────────────────────────────────────────────────────
    completed_txn = txn.filter(F.col("status") == "COMPLETED")

    ref_date_row = completed_txn.agg(F.max(F.col("order_date").cast("date"))).collect()[0][0]
    if ref_date_row is None:
        print("[UserSegmentation] ERROR: No completed transactions found!")
        spark.stop()
        sys.exit(1)

    ref_date = ref_date_row
    cutoff_date = ref_date - timedelta(days=90)
    print(f"[UserSegmentation] ref_date={ref_date}, cutoff={cutoff_date}")

    # Recency: days since last transaction
    recency_df = (completed_txn
                  .groupBy("buyer_id")
                  .agg(F.max(F.col("order_date").cast("date")).alias("last_order_date"))
                  .withColumn("recency_days",
                              F.datediff(F.lit(ref_date), F.col("last_order_date")))
                  .select("buyer_id", "recency_days"))

    # Frequency & Monetary (last 90 days)
    freq_mon_df = (completed_txn
                   .filter(F.col("order_date").cast("date") >= F.lit(cutoff_date))
                   .groupBy("buyer_id")
                   .agg(
                       F.count("*").alias("frequency"),
                       F.sum(F.col("total_amount").cast("double")).alias("monetary")
                   ))

    # Join RFM
    rfm = (recency_df
            .join(freq_mon_df, "buyer_id", "left")
            .fillna({"frequency": 0, "monetary": 0.0})
            .filter(F.col("buyer_id").isNotNull())
            .filter(F.col("recency_days").isNotNull()))

    rfm.cache()
    rfm_count = rfm.count()
    print(f"[UserSegmentation] RFM users: {rfm_count}")

    if rfm_count < 4:
        print("[UserSegmentation] ERROR: Not enough users (need >= 4)")
        spark.stop()
        sys.exit(1)

    # ── Normalize using Z-Score (manual, no numpy needed) ─────────────────────
    stats = rfm.agg(
        F.mean("recency_days").alias("r_mean"), F.stddev("recency_days").alias("r_std"),
        F.mean("frequency").alias("f_mean"), F.stddev("frequency").alias("f_std"),
        F.mean("monetary").alias("m_mean"), F.stddev("monetary").alias("m_std")
    ).collect()[0]

    r_mean, r_std = stats["r_mean"] or 0, stats["r_std"] or 1
    f_mean, f_std = stats["f_mean"] or 0, stats["f_std"] or 1
    m_mean, m_std = stats["m_mean"] or 0, stats["m_std"] or 1

    # Avoid division by zero
    r_std = r_std if r_std > 0 else 1.0
    f_std = f_std if f_std > 0 else 1.0
    m_std = m_std if m_std > 0 else 1.0

    rfm_normalized = (rfm
                      .withColumn("r_scaled", (F.col("recency_days") - F.lit(r_mean)) / F.lit(r_std))
                      .withColumn("f_scaled", (F.col("frequency") - F.lit(f_mean)) / F.lit(f_std))
                      .withColumn("m_scaled", (F.col("monetary") - F.lit(m_mean)) / F.lit(m_std)))

    # ── Segment using composite score (simulates K-Means centroid logic) ──────
    # Score = -recency + frequency + monetary (higher = better customer)
    rfm_scored = rfm_normalized.withColumn(
        "rfm_score", -F.col("r_scaled") + F.col("f_scaled") + F.col("m_scaled")
    )

    # Assign clusters using NTILE(4) on rfm_score
    # This approximates K-Means with k=4 by splitting into equal-sized groups
    rfm_clustered = rfm_scored.withColumn(
        "cluster_id",
        F.ntile(4).over(Window.orderBy(F.desc("rfm_score"))) - 1  # 0-indexed
    )

    # Assign segment labels based on cluster rank
    # cluster_id 0 = highest score = CHAMPION
    # cluster_id 1 = LOYAL
    # cluster_id 2 = AT_RISK
    # cluster_id 3 = lowest score = DORMANT
    result = (rfm_clustered
              .withColumn("segment_label",
                          F.when(F.col("cluster_id") == 0, "CHAMPION")
                          .when(F.col("cluster_id") == 1, "LOYAL")
                          .when(F.col("cluster_id") == 2, "AT_RISK")
                          .otherwise("DORMANT"))
              .select(
                  F.col("buyer_id").alias("user_id"),
                  "recency_days",
                  "frequency",
                  F.round("monetary", 2).alias("monetary"),
                  "cluster_id",
                  "segment_label"
              ))

    # ── Write Output ──────────────────────────────────────────────────────────
    result.coalesce(4).write.mode("overwrite").parquet(args.output_path)

    count = result.count()
    print(f"[UserSegmentation] Written {count} records to {args.output_path}")
    print("[UserSegmentation] Segment distribution:")
    result.groupBy("segment_label").count().orderBy("segment_label").show()

    spark.stop()
    print("[UserSegmentation] DONE")


if __name__ == "__main__":
    main()
