from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F


def with_row_hash(df: DataFrame, attrs: list[str], bits: int = 256) -> DataFrame:
    parts = [F.coalesce(F.col(c).cast("string"), F.lit("~")) for c in attrs]
    return df.withColumn("row_hash", F.sha2(F.concat_ws("||", *parts), bits))


def build_changes(spark: SparkSession, src: DataFrame, target: str, key: str, order_cols: list[str],
                  opts: dict) -> DataFrame:
    eff_from, eff_to, current = opts["effective_from_col"], opts["effective_to_col"], opts["current_flag_col"]
    open_end = F.to_timestamp(F.lit(opts["open_end_date"]))
    order = [F.col(c).asc_nulls_first() for c in order_cols]

    src = src.withColumn("_is_target", F.lit(False))
    if spark.catalog.tableExists(target):
        cur = (spark.table(target).where(F.col(current))
               .select(key, *order_cols, "row_hash", F.lit(True).alias("_is_target")))
        latest = cur.select(key, *[F.col(c).alias(f"_cur_{c}") for c in order_cols])
        src = src.join(latest, key, "left")
        newer = F.lit(False)
        for i, c in enumerate(order_cols):
            tie = F.lit(True)
            for p in order_cols[:i]:
                tie = tie & (F.col(p) == F.col(f"_cur_{p}"))
            newer = newer | (tie & (F.col(c) > F.col(f"_cur_{c}")))
        src = src.where(F.col(f"_cur_{order_cols[0]}").isNull() | newer).drop(*[f"_cur_{c}" for c in order_cols])
        combined = src.unionByName(cur, allowMissingColumns=True)
    else:
        combined = src

    w = Window.partitionBy(key).orderBy(*order)
    changed = (combined
               .withColumn("_prev_hash", F.lag("row_hash").over(w))
               .where(F.col("_is_target") | F.col("_prev_hash").isNull() | (F.col("_prev_hash") != F.col("row_hash")))
               .withColumn(eff_from, F.coalesce("last_updated", "extraction_timestamp"))
               .withColumn("_next_from", F.lead(eff_from).over(w))
               .drop("_prev_hash"))
    has_new = Window.partitionBy(key)
    return (changed
            .withColumn("_has_new", F.max(F.when(~F.col("_is_target"), 1).otherwise(0)).over(has_new))
            .where("_has_new = 1")
            .withColumn(eff_to, F.coalesce("_next_from", open_end))
            .withColumn(current, F.col("_next_from").isNull())
            .drop("_has_new", "_next_from"))


def merge_scd2(spark: SparkSession, changes: DataFrame, target: str, key: str, opts: dict) -> dict:
    eff_from, eff_to, current = opts["effective_from_col"], opts["effective_to_col"], opts["current_flag_col"]
    changes = changes.withColumn("silver_updated_at", F.current_timestamp())
    inserts = changes.where(~F.col("_is_target")).drop("_is_target")
    closes = changes.where(F.col("_is_target") & ~F.col(current)).select(key, eff_to)
    inserts = inserts.withColumn("sk", F.sha2(F.concat_ws("||", key, eff_from, "row_hash"), 256))

    stats = {"inserted": inserts.count(), "closed": closes.count()}
    if not spark.catalog.tableExists(target):
        inserts.write.format("delta").saveAsTable(target)
        return stats

    cols = inserts.columns
    src = (closes.withColumn("_merge_key", F.col(key))
           .unionByName(inserts.withColumn("_merge_key", F.lit(None).cast("string")), allowMissingColumns=True))
    src.createOrReplaceTempView("_scd2_src")
    spark.sql(f"""
        MERGE INTO {target} t
        USING _scd2_src s ON t.{key} = s._merge_key AND t.{current}
        WHEN MATCHED THEN UPDATE SET t.{eff_to} = s.{eff_to}, t.{current} = false, t.silver_updated_at = current_timestamp()
        WHEN NOT MATCHED AND s._merge_key IS NULL THEN INSERT ({", ".join(cols)}) VALUES ({", ".join("s." + c for c in cols)})
    """)
    return stats
