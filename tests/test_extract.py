import gzip
from datetime import date

from ghpipe.extract.download import gzip_ok, hour_keys
from ghpipe.manifest import connect, day_summary, get_row, upsert


def test_hour_keys_unpadded_and_complete():
    keys = list(hour_keys(date(2025, 8, 15), date(2025, 8, 16)))
    assert len(keys) == 48
    assert keys[0] == "2025-08-15-0"
    assert keys[9] == "2025-08-15-9"      # not "09"
    assert keys[-1] == "2025-08-16-23"


def test_gzip_ok_detects_truncation(tmp_path):
    good = tmp_path / "a.json.gz"
    with gzip.open(good, "wt") as f:
        for i in range(20000):
            f.write(f"line {i}\n")
    assert gzip_ok(good)
    bad = tmp_path / "b.json.gz"
    bad.write_bytes(good.read_bytes()[:-20])
    assert not gzip_ok(bad)


def test_manifest_upsert_is_idempotent_and_summarizes(tmp_path):
    conn = connect(tmp_path / "m.db")
    base = dict(url="u", expected_bytes=1, actual_bytes=1, sha256="x", attempts=1)
    upsert(conn, hour_key="2025-08-15-0", status="FAILED", **base)
    upsert(conn, hour_key="2025-08-15-0", status="DOWNLOADED", **base)   # same key again
    upsert(conn, hour_key="2025-08-15-1", status="MISSING", **base)
    assert get_row(conn, "2025-08-15-0")["status"] == "DOWNLOADED"
    s = day_summary(conn)
    assert len(s) == 1 and (s[0]["downloaded"], s[0]["missing"]) == (1, 1)
