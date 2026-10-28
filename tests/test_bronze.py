import gzip
import json
from datetime import date

import pytest

from ghpipe.bronze import process_bronze


def event(id_, type_, created_at="2025-08-15T01:00:01Z"):
    return json.dumps({
        "id": id_, "type": type_,
        "actor": {"id": 1, "login": "alice"},
        "repo": {"id": 10, "name": "owner/repo"},
        "payload": {"action": "started"},
        "public": True, "created_at": created_at,
    })


def write_gz(path, lines):
    with gzip.open(path, "wt") as f:
        f.write("\n".join(lines) + "\n")


def run(spark, raw_dir, tmp_path):
    return process_bronze(spark, f"{raw_dir}/*.json.gz",
                          str(tmp_path / "bronze"), str(tmp_path / "quarantine"))


@pytest.fixture
def raw_dir(tmp_path):
    d = tmp_path / "raw"
    d.mkdir()
    write_gz(d / "2025-08-15-01.json.gz", [
        event("1", "PushEvent"),
        event("2", "IssueCommentEvent"),      # out of scope
        "{bad json completely broken",        # corrupt
    ])
    return d


def test_counts(spark, raw_dir, tmp_path):
    r = run(spark, raw_dir, tmp_path)
    assert (r["in_scope"], r["out_of_scope"], r["corrupt"]) == (1, 1, 1)
    assert r["dates"] == [date(2025, 8, 15)]


def test_only_in_scope_rows_written(spark, raw_dir, tmp_path):
    run(spark, raw_dir, tmp_path)
    rows = spark.read.parquet(str(tmp_path / "bronze")).collect()
    assert [r["type"] for r in rows] == ["PushEvent"]


def test_payload_kept_as_json_string_and_lineage_columns(spark, raw_dir, tmp_path):
    run(spark, raw_dir, tmp_path)
    row = spark.read.parquet(str(tmp_path / "bronze")).first()
    assert json.loads(row["payload"])["action"] == "started"
    assert row["source_file"].endswith("2025-08-15-01.json.gz")
    assert row["event_hour"] == 1
    assert row["event_date"] == date(2025, 8, 15)
    assert row["ingest_ts"] is not None


def test_corrupt_line_goes_to_quarantine(spark, raw_dir, tmp_path):
    run(spark, raw_dir, tmp_path)
    q = spark.read.parquet(str(tmp_path / "quarantine")).collect()
    assert len(q) == 1
    assert "bad json" in q[0]["_corrupt_record"]


def test_no_quarantine_written_when_no_corrupt_rows(spark, tmp_path):
    d = tmp_path / "raw"
    d.mkdir()
    write_gz(d / "2025-08-15-02.json.gz", [event("1", "WatchEvent")])
    r = run(spark, d, tmp_path)
    assert r["corrupt"] == 0
    assert not (tmp_path / "quarantine").exists()


def test_partition_date_comes_from_filename_not_created_at(spark, tmp_path):
    d = tmp_path / "raw"
    d.mkdir()
    # Event stamped the next day sits in the 23:00 file of Aug 15
    write_gz(d / "2025-08-15-23.json.gz",
             [event("1", "PushEvent", created_at="2025-08-16T00:00:01Z")])
    r = run(spark, d, tmp_path)
    assert r["dates"] == [date(2025, 8, 15)]
    assert not (tmp_path / "bronze" / "event_date=2025-08-16").exists()


def test_rerun_is_idempotent(spark, raw_dir, tmp_path):
    run(spark, raw_dir, tmp_path)
    run(spark, raw_dir, tmp_path)
    assert spark.read.parquet(str(tmp_path / "bronze")).count() == 1
    assert spark.read.parquet(str(tmp_path / "quarantine")).count() == 1