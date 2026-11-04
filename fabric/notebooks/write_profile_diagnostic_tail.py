"""Unchanged-row MERGE comparison; no Kafka input or scoring API calls."""

profile = {"runId": RUN_ID, "noKafkaSends": True, "noApiCalls": True,
           "latencyAcceptance": False, "tables": [], "stageTimings": stage_timings}
optimize_profile = "<injected-optimize-write-profile>" == "true"
profile["optimizeUnchangedData"] = optimize_profile
try:
    profile["sessionAllocation"] = _verify_session_allocation()
    for table in (TABLE_EVENTS, TABLE_SCORES, TABLE_AUDIT):
        detail = spark.sql(f"DESCRIBE DETAIL {table}").first().asDict(recursive=True)
        history_before = [row.asDict(recursive=True)
                          for row in spark.sql(f"DESCRIBE HISTORY {table} LIMIT 3").collect()]
        rows = [spark.table(table).where(
            F.col("run_id") == "dec6911e-35a0-4ab0-9482-4b2489ae7f58").limit(1)
                .first().asDict(recursive=True)]
        frame = spark.createDataFrame(rows, spark.table(table).schema)
        frame.coalesce(1).createOrReplaceTempView("unchanged_merge_profile")
        key = "t.run_id=s.run_id AND t.message_id=s.message_id"
        # UUIDs come from the retained valid baseline row, not user-supplied SQL.
        uuid.UUID(rows[0]["run_id"])
        uuid.UUID(rows[0]["message_id"])
        pruned = (key + f" AND t.run_id='{rows[0]['run_id']}'"
                  + f" AND t.message_id='{rows[0]['message_id']}'")
        comparisons = []
        variants = (("before_optimize", pruned), ("before_optimize_repeat", pruned),
                    ("after_optimize", pruned), ("after_optimize_repeat", pruned)) if optimize_profile else (
                        ("dynamic_single_partition", key), ("static_single_partition", pruned),
                        ("dynamic_single_partition_repeat", key),
                        ("static_single_partition_repeat", pruned))
        optimization = None
        for label, condition in variants:
            if label == "after_optimize":
                retained = spark.table(table)
                before = spark.createDataFrame(retained.collect(), retained.schema).cache()
                try:
                    before_count = before.count()
                    started = time.monotonic()
                    metrics = [row.asDict(recursive=True)
                               for row in spark.sql(f"OPTIMIZE {table}").collect()]
                    optimize_seconds = time.monotonic() - started
                    after = spark.table(table)
                    after_count = after.count()
                    if (before_count != after_count or before.exceptAll(after).limit(1).count()
                            or after.exceptAll(before).limit(1).count()):
                        raise RuntimeError(f"OptimizeChangedBusinessRows:{table}")
                    optimization = {
                        "elapsedSeconds": optimize_seconds, "metrics": metrics,
                        "beforeCount": before_count, "afterCount": after_count,
                        "allRowsUnchanged": True,
                        "detailAfter": spark.sql(f"DESCRIBE DETAIL {table}").first().asDict(recursive=True)}
                finally:
                    before.unpersist()
            statement = (f"MERGE INTO {table} t USING unchanged_merge_profile s ON {condition} "
                         "WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *")
            plan = "\n".join(row[0] for row in spark.sql("EXPLAIN FORMATTED " + statement).collect())
            started = time.monotonic()
            spark.sql(statement).collect()
            comparisons.append({
                "kind": label, "elapsedSeconds": time.monotonic() - started, "plan": plan,
                "commit": spark.sql(f"DESCRIBE HISTORY {table} LIMIT 1").first().asDict(recursive=True)})
        profile["tables"].append({
            "table": table, "detail": detail, "historyBefore": history_before,
            "sourcePartitions": frame.coalesce(1).rdd.getNumPartitions(),
            "optimization": optimization, "comparisons": comparisons})
except Exception as error:
    profile["error"] = {"type": type(error).__name__, "message": str(error)[:2000]}


def encode_profile(value):
    if isinstance(value, datetime):
        return value.replace(tzinfo=timezone.utc).isoformat() if value.tzinfo is None else value.isoformat()
    raise TypeError(type(value).__name__)


encoded = json.dumps(profile, default=encode_profile)
notebookutils.fs.put(f"Files/diagnostics/functional-processing-{RUN_ID}.json", encoded, True)
notebookutils.notebook.exit(encoded)
