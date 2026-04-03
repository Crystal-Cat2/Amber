"""成功 request 分层分析独立页回归测试。"""

from __future__ import annotations

import importlib.util
from pathlib import Path
import unittest


REPO_ROOT = Path(__file__).resolve().parents[1]
PROJECT_DIR = REPO_ROOT / "projects" / "success_request_dashboard"
SCRIPT_PATH = PROJECT_DIR / "scripts" / "build_success_request_dashboard.py"
RUNNER_PATH = PROJECT_DIR / "scripts" / "run_success_request_dashboard.py"
SQL_DIR = PROJECT_DIR / "sql"


def load_builder_module():
    """按需加载 builder 模块，避免文件缺失时提前中断发现。"""
    spec = importlib.util.spec_from_file_location("build_success_request_dashboard", SCRIPT_PATH)
    module = importlib.util.module_from_spec(spec)
    assert spec is not None and spec.loader is not None
    spec.loader.exec_module(module)
    return module


class SuccessRequestDashboardTests(unittest.TestCase):
    """校验独立页 SQL、payload 与 eCPM 分桶的核心行为。"""

    def test_project_files_exist(self) -> None:
        self.assertTrue(PROJECT_DIR.exists())
        self.assertTrue(SCRIPT_PATH.exists())
        self.assertTrue(RUNNER_PATH.exists())
        self.assertTrue((SQL_DIR / "success_request_scope_summary.sql").exists())
        self.assertTrue((SQL_DIR / "success_request_cnt_distribution.sql").exists())
        self.assertTrue((SQL_DIR / "success_request_channel_distribution.sql").exists())
        self.assertTrue((SQL_DIR / "success_request_rank_distribution.sql").exists())
        self.assertTrue((SQL_DIR / "success_request_ecpm_distribution.sql").exists())

    def test_cnt_distribution_sql_counts_all_three_statuses_for_bucket(self) -> None:
        sql_text = (SQL_DIR / "success_request_cnt_distribution.sql").read_text(encoding="utf-8")

        self.assertIn("status IN ('AD_LOADED', 'FAILED_TO_LOAD', 'AD_LOAD_NOT_ATTEMPTED')", sql_text)
        self.assertIn("COUNT(DISTINCT CONCAT(network_type, '||', network)) AS network_cnt", sql_text)
        self.assertIn("COUNT(*) AS placement_cnt", sql_text)
        self.assertIn("'network' AS cnt_type", sql_text)
        self.assertIn("'placement' AS cnt_type", sql_text)
        self.assertIn("success_request_cnt", sql_text)

    def test_channel_and_rank_sql_use_first_success_row_and_aggregate_results(self) -> None:
        channel_sql = (SQL_DIR / "success_request_channel_distribution.sql").read_text(encoding="utf-8")
        rank_sql = (SQL_DIR / "success_request_rank_distribution.sql").read_text(encoding="utf-8")

        self.assertIn("ROW_NUMBER() OVER (", channel_sql)
        self.assertIn("ORDER BY event_timestamp ASC", channel_sql)
        self.assertIn("WHERE success_row_num = 1", channel_sql)
        self.assertIn("CONCAT(success_network_type, ' / ', success_network) AS success_target", channel_sql)
        self.assertIn("status = 'FAILED_TO_LOAD'", rank_sql)
        self.assertIn("attempt_rank", rank_sql)
        self.assertIn("success_rank", rank_sql)
        self.assertIn("success_network_type", rank_sql)
        self.assertNotIn("user_pseudo_id", rank_sql.split("SELECT")[-1])

    def test_ecpm_sql_aggregates_by_rounded_price_without_request_level_ids(self) -> None:
        sql_text = (SQL_DIR / "success_request_ecpm_distribution.sql").read_text(encoding="utf-8")

        self.assertIn("ROUND(ecpm, 2) AS rounded_ecpm", sql_text)
        self.assertIn("COUNT(*) AS request_pv", sql_text)
        self.assertIn("cnt_type", sql_text)
        self.assertIn("cnt_value", sql_text)
        self.assertNotIn("SELECT\n  user_pseudo_id", sql_text)
        self.assertNotIn("SELECT\n  request_id", sql_text)

    def test_runner_registers_all_sql_outputs(self) -> None:
        self.assertTrue(RUNNER_PATH.exists())
        spec = importlib.util.spec_from_file_location("run_success_request_dashboard", RUNNER_PATH)
        runner = importlib.util.module_from_spec(spec)
        assert spec is not None and spec.loader is not None
        spec.loader.exec_module(runner)

        self.assertEqual(
            runner.SQL_TO_CSV["success_request_scope_summary.sql"],
            "success_request_scope_summary.csv",
        )
        self.assertEqual(
            runner.SQL_TO_CSV["success_request_ecpm_distribution.sql"],
            "success_request_ecpm_distribution.csv",
        )

    def test_top_countries_are_ranked_by_combined_success_requests_and_units_follow_country(self) -> None:
        self.assertTrue(SCRIPT_PATH.exists())
        dashboard = load_builder_module()

        scope_rows = [
            {"product": "demo", "ad_format": "interstitial", "experiment_group": "no_is_adx", "country": "US", "max_unit_id": "u1", "success_request_cnt": "40"},
            {"product": "demo", "ad_format": "interstitial", "experiment_group": "have_is_adx", "country": "US", "max_unit_id": "u2", "success_request_cnt": "20"},
            {"product": "demo", "ad_format": "interstitial", "experiment_group": "no_is_adx", "country": "JP", "max_unit_id": "u3", "success_request_cnt": "30"},
            {"product": "demo", "ad_format": "interstitial", "experiment_group": "have_is_adx", "country": "JP", "max_unit_id": "u4", "success_request_cnt": "25"},
            {"product": "demo", "ad_format": "interstitial", "experiment_group": "no_is_adx", "country": "BR", "max_unit_id": "u5", "success_request_cnt": "10"},
        ]

        country_options = dashboard.build_top_countries_by_combo(scope_rows, limit=2)
        unit_options = dashboard.build_unit_options_by_country(scope_rows, country_options)

        self.assertEqual(country_options["demo__interstitial"], ["US", "JP"])
        self.assertEqual(unit_options["demo__interstitial__US"], ["u1", "u2"])
        self.assertEqual(unit_options["demo__interstitial__JP"], ["u3", "u4"])
        self.assertNotIn("demo__interstitial__BR", unit_options)

    def test_unit_labels_prefer_readable_name_and_fallback_to_id(self) -> None:
        dashboard = load_builder_module()

        rows = [
            {"max_unit_id": "u1"},
            {"max_unit_id": "u2"},
            {"max_unit_id": ""},
        ]
        labeled = dashboard.attach_unit_labels(rows, {"u1": "Readable Unit"})

        self.assertEqual(labeled[0]["ad_unit_name"], "Readable Unit")
        self.assertEqual(labeled[1]["ad_unit_name"], "u2")
        self.assertEqual(labeled[2]["ad_unit_name"], "UNKNOWN_UNIT")

    def test_unit_options_are_sorted_by_tail_p_then_df(self) -> None:
        dashboard = load_builder_module()

        scope_rows = [
            {"product": "demo", "ad_format": "rewarded", "country": "US", "max_unit_id": "u_df", "ad_unit_name": "Nice DF", "success_request_cnt": "10"},
            {"product": "demo", "ad_format": "rewarded", "country": "US", "max_unit_id": "u_p3", "ad_unit_name": "Nice P3", "success_request_cnt": "99"},
            {"product": "demo", "ad_format": "rewarded", "country": "US", "max_unit_id": "u_other", "ad_unit_name": "Nice ZZ", "success_request_cnt": "999"},
            {"product": "demo", "ad_format": "rewarded", "country": "US", "max_unit_id": "u_p1", "ad_unit_name": "Nice P1", "success_request_cnt": "1"},
        ]

        country_options = {"demo__rewarded": ["US"]}
        unit_options = dashboard.build_unit_options_by_country(scope_rows, country_options)

        self.assertEqual(unit_options["demo__rewarded__US"], ["u_p1", "u_p3", "u_df", "u_other"])

    def test_cnt_distribution_rolls_up_35_plus_and_computes_share_diff(self) -> None:
        dashboard = load_builder_module()

        cnt_rows = [
            {"product": "demo", "ad_format": "rewarded", "country": "US", "max_unit_id": "u1", "cnt_type": "network", "cnt_value": "1", "experiment_group": "no_is_adx", "success_request_cnt": "30"},
            {"product": "demo", "ad_format": "rewarded", "country": "US", "max_unit_id": "u1", "cnt_type": "network", "cnt_value": "38", "experiment_group": "no_is_adx", "success_request_cnt": "70"},
            {"product": "demo", "ad_format": "rewarded", "country": "US", "max_unit_id": "u1", "cnt_type": "network", "cnt_value": "1", "experiment_group": "have_is_adx", "success_request_cnt": "60"},
            {"product": "demo", "ad_format": "rewarded", "country": "US", "max_unit_id": "u1", "cnt_type": "network", "cnt_value": "45", "experiment_group": "have_is_adx", "success_request_cnt": "40"},
        ]

        distribution = dashboard.build_share_distribution_rows(
            cnt_rows,
            dimension_fields=["product", "ad_format", "country", "max_unit_id", "cnt_type"],
            category_field="cnt_value",
            count_field="success_request_cnt",
            category_transform=dashboard.normalize_cnt_bucket,
        )

        self.assertEqual([row["category_label"] for row in distribution], ["1", "35+"])
        bucket_35 = distribution[1]
        self.assertEqual(bucket_35["request_pv_a"], 70)
        self.assertEqual(bucket_35["request_pv_b"], 40)
        self.assertAlmostEqual(bucket_35["share_a"], 0.7)
        self.assertAlmostEqual(bucket_35["share_b"], 0.4)
        self.assertAlmostEqual(bucket_35["share_diff"], -0.3)

    def test_ecpm_bucket_builder_uses_adaptive_non_equal_width_ranges(self) -> None:
        self.assertTrue(SCRIPT_PATH.exists())
        dashboard = load_builder_module()

        freq_rows = [
            {"rounded_ecpm": "0.01", "request_pv": "120"},
            {"rounded_ecpm": "0.02", "request_pv": "110"},
            {"rounded_ecpm": "0.10", "request_pv": "80"},
            {"rounded_ecpm": "1.25", "request_pv": "35"},
            {"rounded_ecpm": "3.75", "request_pv": "20"},
            {"rounded_ecpm": "9.50", "request_pv": "10"},
        ]

        buckets = dashboard.build_adaptive_ecpm_buckets(freq_rows, max_buckets=4)

        self.assertGreaterEqual(len(buckets), 3)
        labels = [bucket["bucket_label"] for bucket in buckets]
        self.assertEqual(len(labels), len(set(labels)))
        widths = [round(bucket["bucket_max"] - bucket["bucket_min"], 2) for bucket in buckets]
        self.assertGreater(len(set(widths)), 1)
        self.assertTrue(any(label.startswith("0.") or label.startswith("1.") or label.endswith("+") for label in labels))

    def test_ecpm_distribution_is_adaptive_per_slice_not_global_fixed_bounds(self) -> None:
        dashboard = load_builder_module()

        tmpdir = REPO_ROOT / "tmp_test" / "success_request_dashboard_tests"
        tmpdir.mkdir(parents=True, exist_ok=True)
        csv_path = tmpdir / "ecpm.csv"
        try:
            csv_path.write_text(
                "\n".join(
                    [
                        "product,ad_format,experiment_group,country,max_unit_id,cnt_type,cnt_value,rounded_ecpm,request_pv",
                        "demo,rewarded,no_is_adx,US,u1,network,1,0.01,100",
                        "demo,rewarded,have_is_adx,US,u1,network,1,0.50,100",
                        "demo,rewarded,no_is_adx,US,u2,network,1,200.00,120",
                        "demo,rewarded,have_is_adx,US,u2,network,1,500.00,80",
                    ]
                ),
                encoding="utf-8",
            )

            rows = dashboard.build_ecpm_distribution_rows_from_csv(csv_path, {"demo__rewarded": ["US"]})
        finally:
            csv_path.unlink(missing_ok=True)

        u1_labels = [row["category_label"] for row in rows if row["max_unit_id"] == "u1"]
        u2_labels = [row["category_label"] for row in rows if row["max_unit_id"] == "u2"]
        self.assertTrue(u1_labels)
        self.assertTrue(u2_labels)
        self.assertNotEqual(u1_labels, u2_labels)
        self.assertFalse(any(label == "100+" for label in u2_labels))

    def test_ecpm_summary_rows_are_weighted_from_raw_price_frequency(self) -> None:
        dashboard = load_builder_module()

        tmpdir = REPO_ROOT / "tmp_test" / "success_request_dashboard_tests"
        tmpdir.mkdir(parents=True, exist_ok=True)
        csv_path = tmpdir / "ecpm_summary.csv"
        try:
            csv_path.write_text(
                "\n".join(
                    [
                        "product,ad_format,experiment_group,country,max_unit_id,cnt_type,cnt_value,rounded_ecpm,request_pv",
                        "demo,rewarded,no_is_adx,US,u1,network,1,10.00,2",
                        "demo,rewarded,no_is_adx,US,u1,network,1,20.00,1",
                        "demo,rewarded,have_is_adx,US,u1,network,1,30.00,3",
                        "demo,rewarded,have_is_adx,US,u1,network,1,60.00,1",
                    ]
                ),
                encoding="utf-8",
            )

            _, summary_rows = dashboard.build_ecpm_distribution_and_summary_from_csv(csv_path, {"demo__rewarded": ["US"]})
        finally:
            csv_path.unlink(missing_ok=True)

        self.assertEqual(len(summary_rows), 1)
        summary = summary_rows[0]
        self.assertAlmostEqual(summary["mean_a"], 40 / 3)
        self.assertAlmostEqual(summary["mean_b"], 150 / 4)
        self.assertAlmostEqual(summary["mean_diff"], (150 / 4) - (40 / 3))

    def test_rank_distribution_rows_include_bidding_waterfall_stack_and_mean(self) -> None:
        dashboard = load_builder_module()

        rank_rows, summary_rows = dashboard.build_rank_distribution_and_summary_rows(
            [
                {"product": "demo", "ad_format": "rewarded", "country": "US", "max_unit_id": "u1", "ad_unit_name": "U1", "cnt_type": "network", "cnt_value": "1", "experiment_group": "no_is_adx", "success_rank": "1", "success_network_type": "bidding", "request_pv": "4"},
                {"product": "demo", "ad_format": "rewarded", "country": "US", "max_unit_id": "u1", "ad_unit_name": "U1", "cnt_type": "network", "cnt_value": "1", "experiment_group": "no_is_adx", "success_rank": "2", "success_network_type": "waterfall", "request_pv": "6"},
                {"product": "demo", "ad_format": "rewarded", "country": "US", "max_unit_id": "u1", "ad_unit_name": "U1", "cnt_type": "network", "cnt_value": "1", "experiment_group": "have_is_adx", "success_rank": "1", "success_network_type": "bidding", "request_pv": "3"},
                {"product": "demo", "ad_format": "rewarded", "country": "US", "max_unit_id": "u1", "ad_unit_name": "U1", "cnt_type": "network", "cnt_value": "1", "experiment_group": "have_is_adx", "success_rank": "1", "success_network_type": "waterfall", "request_pv": "1"},
            ]
        )

        self.assertEqual([row["category_label"] for row in rank_rows], ["1", "2"])
        rank1 = rank_rows[0]
        self.assertAlmostEqual(rank1["share_a"], 0.4)
        self.assertAlmostEqual(rank1["share_b"], 1.0)
        self.assertEqual(rank1["bidding_request_pv_a"], 4)
        self.assertEqual(rank1["waterfall_request_pv_a"], 0)
        self.assertEqual(rank1["bidding_request_pv_b"], 3)
        self.assertEqual(rank1["waterfall_request_pv_b"], 1)
        self.assertAlmostEqual(rank1["bidding_share_a"], 0.4)
        self.assertAlmostEqual(rank1["waterfall_share_b"], 0.25)
        self.assertEqual(len(summary_rows), 1)
        self.assertAlmostEqual(summary_rows[0]["mean_a"], 1.6)
        self.assertAlmostEqual(summary_rows[0]["mean_b"], 1.0)

    def test_ecpm_bucket_builder_prefers_more_granular_splits_for_dense_prices(self) -> None:
        dashboard = load_builder_module()

        freq_rows = [
            {"rounded_ecpm": "10.00", "request_pv": "50"},
            {"rounded_ecpm": "10.10", "request_pv": "40"},
            {"rounded_ecpm": "10.20", "request_pv": "35"},
            {"rounded_ecpm": "10.30", "request_pv": "30"},
            {"rounded_ecpm": "10.40", "request_pv": "28"},
            {"rounded_ecpm": "11.50", "request_pv": "20"},
            {"rounded_ecpm": "12.00", "request_pv": "18"},
            {"rounded_ecpm": "15.00", "request_pv": "10"},
        ]

        buckets = dashboard.build_adaptive_ecpm_buckets(freq_rows)

        self.assertGreaterEqual(len(buckets), 5)

    def test_render_html_keeps_metrics_in_tooltip_not_bar_labels(self) -> None:
        dashboard = load_builder_module()

        html = dashboard.render_html(
            {
                "groupLabels": {"no_is_adx": "A组", "have_is_adx": "B组"},
                "groupOrder": ["no_is_adx", "have_is_adx"],
                "combos": [],
                "countryOptions": {},
                "unitOptions": {},
                "unitLabels": {},
                "scopeRows": [],
                "cntRows": [],
                "channelRows": [],
                "rankRows": [],
                "rankSummaryRows": [],
                "ecpmBucketRows": [],
                "ecpmSummaryRows": [],
            }
        )

        self.assertIn("tooltip:", html)
        self.assertNotIn("label: {", html)
        self.assertIn("ecpmSummary", html)
        self.assertIn("rankSummary", html)

    def test_render_html_uses_high_contrast_rank_stack_colors(self) -> None:
        dashboard = load_builder_module()

        html = dashboard.render_html(
            {
                "groupLabels": {"no_is_adx": "A组", "have_is_adx": "B组"},
                "groupOrder": ["no_is_adx", "have_is_adx"],
                "combos": [],
                "countryOptions": {},
                "unitOptions": {},
                "unitLabels": {},
                "scopeRows": [],
                "cntRows": [],
                "channelRows": [],
                "rankRows": [],
                "rankSummaryRows": [],
                "ecpmBucketRows": [],
                "ecpmSummaryRows": [],
            }
        )

        self.assertIn("color: ['#1d4ed8', '#b91c1c', '#60a5fa', '#f59e0b']", html)


if __name__ == "__main__":
    unittest.main()
