"""IsAdx request latency 分布 SQL 回归测试。"""

from __future__ import annotations

from pathlib import Path
import unittest


REPO_ROOT = Path(__file__).resolve().parents[1]
SQL_PATH = REPO_ROOT / "outputs" / "sql" / "isadx_latency_distribution_by_unit_20260105_20260112.sql"


class IsadxLatencyDistributionByUnitSqlTests(unittest.TestCase):
    """校验 request 级 isadx latency 分布 SQL 的关键口径。"""

    def test_sql_file_exists(self) -> None:
        self.assertTrue(SQL_PATH.exists())

    def test_sql_uses_confirmed_detail_fields_and_filters_isadx_rows(self) -> None:
        sql_text = SQL_PATH.read_text(encoding="utf-8")

        self.assertIn("`commercial-adx.lmh.isadx_adslog_latency_detail`", sql_text)
        self.assertIn("network = 'IsAdxCustomAdapter'", sql_text)
        self.assertIn("event_time_utc >= TIMESTAMP('2026-01-05 00:00:00+00')", sql_text)
        self.assertIn("event_time_utc < TIMESTAMP('2026-01-13 00:00:00+00')", sql_text)
        self.assertIn("SUM(latency) AS request_latency_sec", sql_text)
        self.assertIn("MAX(CASE WHEN status = 'AD_LOADED' THEN 1 ELSE 0 END) AS has_loaded", sql_text)
        self.assertIn("MAX(CASE WHEN status = 'FAILED_TO_LOAD' THEN 1 ELSE 0 END) AS has_failed", sql_text)

    def test_sql_builds_success_fail_request_status_and_drops_other_cases(self) -> None:
        sql_text = SQL_PATH.read_text(encoding="utf-8")

        self.assertIn("WHEN has_loaded = 1 THEN 'success'", sql_text)
        self.assertIn("WHEN has_loaded = 0 AND has_failed = 1 THEN 'fail'", sql_text)
        self.assertIn("END AS request_status", sql_text)
        self.assertIn("WHERE request_status IS NOT NULL", sql_text)

    def test_sql_rounds_to_two_decimals_and_rolls_gt_30_into_30_plus(self) -> None:
        sql_text = SQL_PATH.read_text(encoding="utf-8")

        self.assertIn("ROUND(request_latency_sec, 2) AS rounded_latency_sec", sql_text)
        self.assertIn("WHEN rounded_latency_sec > 30.00 THEN '30+'", sql_text)
        self.assertIn("FORMAT('%.2f', rounded_latency_sec)", sql_text)
        self.assertIn("AS latency_bucket", sql_text)

    def test_sql_outputs_distribution_on_unit_and_status_grain_with_bucket_order(self) -> None:
        sql_text = SQL_PATH.read_text(encoding="utf-8")

        self.assertIn("product", sql_text)
        self.assertIn("ad_format", sql_text)
        self.assertIn("max_unit_id", sql_text)
        self.assertIn("request_status", sql_text)
        self.assertIn("latency_bucket", sql_text)
        self.assertIn("COUNT(*) AS request_pv", sql_text)
        self.assertIn("GROUP BY product, ad_format, max_unit_id, request_status, latency_bucket", sql_text)
        self.assertIn("WHEN latency_bucket = '30+' THEN 999999", sql_text)
        self.assertIn("ELSE SAFE_CAST(latency_bucket AS FLOAT64)", sql_text)


if __name__ == "__main__":
    unittest.main()
