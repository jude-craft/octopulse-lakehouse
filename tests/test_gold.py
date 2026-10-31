import pytest
from datetime import date, datetime, timedelta

from ghpipe.gold.contributors import build_contributor_activity_daily
from ghpipe.gold.language_activity import build_language_activity_daily
from ghpipe.gold.pr_health import build_pr_health_daily
from ghpipe.gold.repo_activity import build_repo_activity_daily
from ghpipe.gold.pr_merge_time import build_pr_merge_time_daily

D1, D2, D3 = date(2025, 8, 15), date(2025, 8, 16), date(2025, 8, 17)

PR_SCHEMA = ("event_date date, repo_id long, actor_id long, is_bot boolean, pr_action string, "
             "pr_outcome string, base_language string, merged_at timestamp, pr_created_at timestamp")


def events(spark, rows):
    """rows: (event_date, event_type, repo_id, actor_id, is_bot)"""
    data = [(d, t, r, a, b, f"repo{r}", f"user{a}", datetime(d.year, d.month, d.day, 12))
            for d, t, r, a, b in rows]
    return spark.createDataFrame(
        data, "event_date date, event_type string, repo_id long, actor_id long, is_bot boolean, "
              "repo_name string, actor_login string, event_ts timestamp")


def stars(day, n, repo=1):
    return [(day, "WatchEvent", repo, i, False) for i in range(n)]


def test_star_velocity_with_complete_history(spark):
    out = {r["event_date"]: r for r in
           build_repo_activity_daily(events(spark, stars(D1, 5) + stars(D2, 10)), 1, 5).collect()}
    assert out[D1]["star_velocity"] is None               # no day before D1 is loaded
    assert out[D2]["star_events_prior_window"] == 5
    assert out[D2]["star_velocity"] == 2.0
    assert out[D2]["trending_rank"] == 1


def test_gap_day_blocks_velocity(spark):
    out = {r["event_date"]: r for r in
           build_repo_activity_daily(events(spark, stars(D1, 5) + stars(D3, 10)), 1, 5).collect()}
    assert out[D3]["star_velocity"] is None               # D2 is not loaded


def test_low_prior_volume_gives_no_velocity(spark):
    out = {r["event_date"]: r for r in
           build_repo_activity_daily(events(spark, stars(D1, 2) + stars(D2, 10)), 1, 5).collect()}
    assert out[D2]["star_velocity"] is None and out[D2]["star_delta"] == 8


def test_unknown_language_bucket_and_coverage(spark):
    rows = [(D1, 1, 1, False, "opened", None, "Python", None, None),
            (D1, 2, 2, False, "opened", None, "Python", None, None),
            (D1, 3, 3, False, "opened", None, None, None, None)]
    out = {r["language"]: r for r in
           build_language_activity_daily(spark.createDataFrame(rows, PR_SCHEMA)).collect()}
    assert out["Python"]["pr_events"] == 2
    assert out["UNKNOWN"]["is_known_language"] is False
    assert out["Python"]["language_coverage"] == pytest.approx(2 / 3)
    assert out["Python"]["share_of_pr_events"] == pytest.approx(2 / 3)


def test_new_contributor_flag(spark):
    rows = [(D1, "PushEvent", 1, 1, False), (D2, "PushEvent", 1, 1, False), (D2, "PushEvent", 1, 2, False)]
    out = {(r["event_date"], r["actor_id"]): r["is_new_contributor"]
           for r in build_contributor_activity_daily(events(spark, rows)).collect()}
    assert out[(D1, 1)] is None      # first loaded day of the era: cannot know
    assert out[(D2, 1)] is False
    assert out[(D2, 2)] is True
    
def test_merge_time_is_pr_level_and_split_by_actor_type(spark):
    t0 = datetime(2025, 8, 15, 10)

    def row(n, hours, bot):
        return (D1, 1, n, bot, "closed", "merged", None, t0 + timedelta(hours=hours), t0)

    rows = [row(1, 1, False), row(2, 2, False), row(3, 3, False), row(4, 24, False), row(5, 1, True),
            (D1, 1, 6, False, "closed", "closed_unmerged", None, None, t0),   # not merged: excluded
            (D1, 1, 7, False, "merged", "merged", None, None, None)]          # era B style, no times: excluded
    out = {r["is_bot"]: r for r in
           build_pr_merge_time_daily(spark.createDataFrame(rows, PR_SCHEMA)).collect()}
    assert out[False]["merged_prs_with_times"] == 4
    assert out[False]["p50_hours"] == pytest.approx(2.0, abs=1.0)   # percentile_approx returns a sample value
    assert out[True]["merged_prs_with_times"] == 1