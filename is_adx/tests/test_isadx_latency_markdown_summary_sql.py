"""IsAdx latency Markdown 汇总 SQL 回归测试。"""

from __future__ import annotations

from pathlib import Path
import unittest


REPO_ROOT = Path(__file__).resolve().parents[1]
SQL_PATH = REPO_ROOT / "outputs" / "sql" / "isadx_latency_ab_summary_20260105_20260112.sql"


class IsadxLatencyMarkdownSummarySqlTests(unittest.TestCase):
    """校验 Markdown 汇总 SQL 的核心维度与占比口径。"""

    def test_sql_file_exists(self) -> None:
        self.assertTrue(SQL_PATH.exists())

    def test_sql_includes_experiment_group_and_request_status(self) -> None:
        sql_text = SQL_PATH.read_text(encoding="utf-8")

        self.assertIn("experiment_group", sql_text)
        self.assertIn("WHEN has_loaded = 1 THEN 'success'", sql_text)
        self.assertIn("WHEN has_loaded = 0 AND has_failed = 1 THEN 'fail'", sql_text)
        self.assertIn("WHERE request_status IS NOT NULL", sql_text)

    def test_sql_outputs_bucket_counts_and_share_for_markdown_pivot(self) -> None:
        sql_text = SQL_PATH.read_text(encoding="utf-8")

        self.assertIn("latency_bucket", sql_text)
        self.assertIn("COUNT(*) AS request_pv", sql_text)
        self.assertIn("SUM(COUNT(*)) OVER (", sql_text)
        self.assertIn("AS denominator_request_pv", sql_text)
        self.assertIn("SAFE_DIVIDE(", sql_text)
        self.assertIn("AS share", sql_text)
        self.assertIn(
            "GROUP BY product, request_status, latency_bucket, ad_format, experiment_group",
            sql_text,
        )

    def test_sql_aggregates_across_units_for_product_time_rows(self) -> None:
        sql_text = SQL_PATH.read_text(encoding="utf-8")

        self.assertNotIn("SELECT\n  max_unit_id", sql_text)
        self.assertNotIn("GROUP BY product, ad_format, max_unit_id", sql_text)
        self.assertIn("product", sql_text)
        self.assertIn("request_status", sql_text)
        self.assertIn("latency_bucket", sql_text)


if __name__ == "__main__":
    unittest.main()
