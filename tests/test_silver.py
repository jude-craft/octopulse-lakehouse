import gzip
import json

import pytest

from ghpipe.bronze import process_bronze
from ghpipe.silver.pipeline import process_silver_day


def ev(id_, type_, payload, login="alice", created_at="2025-08-15T01:00:01Z"):
    return json.dumps({"id": id_, "type": type_, "actor": {"id": 1, "login": login},
                       "repo": {"id": 10, "name": "owner/repo"}, "payload": payload,
                       "public": True, "created_at": created_at})


def write_gz(path, lines):
    with gzip.open(path, "wt") as f:
        f.write("\n".join(lines) + "\n")


PUSH_A = {"push_id": 1, "size": 4, "distinct_size": 1, "ref": "refs/heads/main", "head": "abc",
          "commits": [{"sha": "abc", "author": {"email": "x@example.com", "name": "x"}}]}


def pr_a(merged, number):
    return {"action": "closed", "number": number,
            "pull_request": {"id": number * 10, "number": number, "merged": merged,
                             "merged_at": "2025-08-15T01:30:00Z" if merged else None,
                             "created_at": "2025-08-14T01:30:00Z",
                             "base": {"repo": {"language": "Python"}}}}


@pytest.fixture(scope="module")
def silver(spark, tmp_path_factory):
    root = tmp_path_factory.mktemp("silver")
    raw = root / "raw"
    raw.mkdir()
    write_gz(raw / "2025-08-15-1.json.gz", [
        ev("1", "PushEvent", PUSH_A),
        ev("1", "PushEvent", PUSH_A),                                   # duplicate
        ev("2", "PullRequestEvent", pr_a(True, 7)),
        ev("3", "PullRequestEvent", pr_a(False, 8)),
        ev("4", "WatchEvent", {"action": "started"}),
        ev("5", "PushEvent", {**PUSH_A, "push_id": 5}, login="dependabot[bot]"),
        ev("6", "PushEvent", {**PUSH_A, "push_id": 6}, login="CustomBot"),   # in known_bots
    ])
    b = "2026-09-15T01:00:01Z"
    write_gz(raw / "2026-09-15-1.json.gz", [
        ev("7", "PullRequestEvent", {"action": "merged", "number": 9,
                                     "pull_request": {"id": 90, "number": 9}}, created_at=b),
        ev("8", "PullRequestEvent", {"action": "closed", "number": 10,
                                     "pull_request": {"id": 100, "number": 10}}, created_at=b),
        ev("9", "PushEvent", {"push_id": 9, "ref": "refs/heads/x", "head": "def"}, created_at=b),
    ])
    (root / "bots.csv").write_text("login\nCustomBot\n")
    process_bronze(spark, f"{raw}/*.json.gz", str(root / "bronze"), str(root / "quarantine"))
    stats = {d: process_silver_day(spark, d, str(root / "bronze"),
                                   str(root / "silver"), str(root / "bots.csv"))
             for d in ("2025-08-15", "2026-09-15")}
    return spark, root, stats


def test_dedup_and_reconciliation(silver):
    _, _, stats = silver
    a = stats["2025-08-15"]
    assert (a["bronze_rows"], a["silver_events"], a["duplicates_removed"]) == (7, 6, 1)


def test_bot_flag(silver):
    spark, root, _ = silver
    rows = {r["event_id"]: r["is_bot"]
            for r in spark.read.parquet(str(root / "silver" / "events")).collect()}
    assert rows["5"] is True and rows["6"] is True and rows["2"] is False


def test_pr_outcomes_and_era_flags(silver):
    spark, root, _ = silver
    rows = spark.read.parquet(str(root / "silver" / "pull_requests")).collect()
    assert {r["pr_number"]: r["pr_outcome"] for r in rows} == {
        7: "merged", 8: "closed_unmerged", 9: "merged", 10: "closed_unmerged"}
    assert {r["pr_number"]: r["outcome_inferred"] for r in rows} == {
        7: False, 8: False, 9: False, 10: True}       
    assert {r["pr_number"]: r["merged_known"] for r in rows} == {
        7: True, 8: True, 9: False, 10: False}
    assert {r["pr_number"]: r["payload_era"] for r in rows} == {
        7: "A_full", 8: "A_full", 9: "B_trimmed", 10: "B_trimmed"}


def test_push_fields_by_era(silver):
    spark, root, _ = silver
    rows = {r["push_id"]: r["distinct_size"]
            for r in spark.read.parquet(str(root / "silver" / "pushes")).collect()}
    assert rows[1] == 1 and rows[9] is None


def test_silver_rerun_is_idempotent(silver):
    spark, root, _ = silver
    process_silver_day(spark, "2025-08-15", str(root / "bronze"),
                       str(root / "silver"), str(root / "bots.csv"))
    assert spark.read.parquet(str(root / "silver" / "events")).count() == 9
    
def test_typed_tables_are_deduplicated(silver):
    spark, root, _ = silver
    pushes = spark.read.parquet(str(root / "silver" / "pushes")).collect()
    # event "1" appears twice in the raw file; after parse-then-dedup it must appear once
    assert sorted(r["push_id"] for r in pushes) == [1, 5, 6, 9]


def test_typed_tables_drop_payload_and_lineage_columns(silver):
    spark, root, _ = silver
    for table in ("pull_requests", "pushes"):
        cols = spark.read.parquet(str(root / "silver" / table)).columns
        assert "payload" not in cols
        assert "source_file" not in cols and "ingest_ts" not in cols


def test_events_table_has_no_payload(silver):
    spark, root, _ = silver
    assert "payload" not in spark.read.parquet(str(root / "silver" / "events")).columns