"""AB dashboard metric5/6 迁移与新页面测试。"""

from __future__ import annotations

import importlib.util
from pathlib import Path
import unittest


REPO_ROOT = Path(__file__).resolve().parents[1]
PROJECT_DIR = REPO_ROOT / "projects" / "ab_dashboard"
SCRIPT_PATH = PROJECT_DIR / "scripts" / "build_ab_share_dashboard.py"
RUNNER_PATH = PROJECT_DIR / "scripts" / "run_ab_dashboard_sql.py"

spec = importlib.util.spec_from_file_location("build_ab_share_dashboard", SCRIPT_PATH)
dashboard = importlib.util.module_from_spec(spec)
assert spec is not None and spec.loader is not None
spec.loader.exec_module(dashboard)

runner_spec = importlib.util.spec_from_file_location("run_ab_dashboard_sql", RUNNER_PATH)
runner = importlib.util.module_from_spec(runner_spec)
assert runner_spec is not None and runner_spec.loader is not None
runner_spec.loader.exec_module(runner)


class AbDashboardMetric56MigrationTests(unittest.TestCase):
    """覆盖 metric5/6 迁移后的核心行为。"""

    def test_request_structure_payload_only_keeps_metric1_to_metric4(self) -> None:
        payload = dashboard.build_request_structure_payload()
        self.assertEqual(set(payload["metrics"].keys()), {"metric1", "metric2", "metric3", "metric4"})

    def test_bidding_network_status_payload_injects_all_unit_before_real_units(self) -> None:
        payload = dashboard.build_bidding_network_status_dashboard_payload(
            rows=[
                {
                    "experiment_group": dashboard.GROUP_A,
                    "product": "com.demo.app",
                    "ad_format": "interstitial",
                    "max_unit_id": "u_p1",
                    "ad_unit_name": "Demo Inter P1",
                    "network_type": "bidding",
                    "network": "Pangle",
                    "status_bucket": "AD_LOADED",
                    "request_pv": "20",
                    "denominator_request_pv": "100",
                    "share": "0.20",
                }
            ],
            configured_units_by_channel={
                ("com.demo.app", "interstitial", "bidding", "Pangle"): {"u_p1"},
            },
        )
        format_payload = payload["platforms"]["android"]["formats"]["interstitial"]
        self.assertEqual(format_payload["unit_options"][0]["value"], dashboard.ALL_UNIT_OPTION_VALUE)
        self.assertEqual(format_payload["unit_options"][0]["label"], "ALL UNIT")
        self.assertEqual(format_payload["default_unit"], dashboard.ALL_UNIT_OPTION_VALUE)
        all_unit = format_payload["unit_map"][dashboard.ALL_UNIT_OPTION_VALUE]
        self.assertAlmostEqual(
            all_unit["network_types"]["bidding"]["groups"][dashboard.GROUP_A]["series"]["AD_LOADED"][0]["request_pv"],
            20.0,
        )
        self.assertAlmostEqual(
            all_unit["network_types"]["bidding"]["groups"][dashboard.GROUP_A]["series"]["AD_LOADED"][0]["denominator_request_pv"],
            100.0,
        )

    def test_bidding_network_status_all_unit_uses_configured_units_and_synthesizes_missing_null(self) -> None:
        payload = dashboard.build_bidding_network_status_payload(
            [
                {
                    "experiment_group": dashboard.GROUP_A,
                    "product": "com.demo.app",
                    "ad_format": "interstitial",
                    "max_unit_id": "u_p1",
                    "ad_unit_name": "Demo Inter P1",
                    "network_type": "bidding",
                    "network": "AdMob",
                    "status_bucket": "AD_LOADED",
                    "request_pv": "10",
                    "denominator_request_pv": "100",
                    "share": "0.10",
                },
                {
                    "experiment_group": dashboard.GROUP_A,
                    "product": "com.demo.app",
                    "ad_format": "interstitial",
                    "max_unit_id": "u_p1",
                    "ad_unit_name": "Demo Inter P1",
                    "network_type": "bidding",
                    "network": "AdMob",
                    "status_bucket": "NULL",
                    "request_pv": "90",
                    "denominator_request_pv": "100",
                    "share": "0.90",
                },
                {
                    "experiment_group": dashboard.GROUP_A,
                    "product": "com.demo.app",
                    "ad_format": "interstitial",
                    "max_unit_id": "u_p2",
                    "ad_unit_name": "Demo Inter P2",
                    "network_type": "bidding",
                    "network": "Pangle",
                    "status_bucket": "AD_LOADED",
                    "request_pv": "50",
                    "denominator_request_pv": "50",
                    "share": "1.00",
                },
            ],
            configured_units_by_channel={
                ("com.demo.app", "interstitial", "bidding", "AdMob"): {"u_p1", "u_p2"},
                ("com.demo.app", "interstitial", "bidding", "Pangle"): {"u_p2"},
            },
        )
        format_payload = payload["platforms"]["android"]["formats"]["interstitial"]
        all_unit = format_payload["unit_map"][dashboard.ALL_UNIT_OPTION_VALUE]
        bidding_block = all_unit["network_types"]["bidding"]["groups"][dashboard.GROUP_A]["series"]
        self.assertAlmostEqual(bidding_block["AD_LOADED"][0]["request_pv"], 10.0)
        self.assertAlmostEqual(bidding_block["AD_LOADED"][0]["denominator_request_pv"], 150.0)
        self.assertAlmostEqual(bidding_block["NULL"][0]["request_pv"], 90.0)
        self.assertAlmostEqual(bidding_block["NULL"][0]["share"], 90.0 / 150.0)

    def test_winning_type_network_payload_builds_all_unit_and_splits_status_columns(self) -> None:
        payload = dashboard.build_winning_type_network_status_payload(
            [
                {
                    "experiment_group": dashboard.GROUP_A,
                    "product": "com.demo.app",
                    "ad_format": "interstitial",
                    "winner_network_type": "bidding",
                    "winner_network": "Pangle",
                    "max_unit_id": "u_p1",
                    "ad_unit_name": "Demo Inter P1",
                    "network_type": "bidding",
                    "network": "AdMob",
                    "status_bucket": "NULL",
                    "request_pv": "3",
                    "denominator_request_pv": "10",
                    "share": "0.30",
                },
                {
                    "experiment_group": dashboard.GROUP_A,
                    "product": "com.demo.app",
                    "ad_format": "interstitial",
                    "winner_network_type": "bidding",
                    "winner_network": "Pangle",
                    "max_unit_id": "u_p2",
                    "ad_unit_name": "Demo Inter P2",
                    "network_type": "bidding",
                    "network": "AdMob",
                    "status_bucket": "AD_LOADED",
                    "request_pv": "2",
                    "denominator_request_pv": "5",
                    "share": "0.40",
                },
                {
                    "experiment_group": dashboard.GROUP_A,
                    "product": "com.demo.app",
                    "ad_format": "interstitial",
                    "winner_network_type": "bidding",
                    "winner_network": "Pangle",
                    "max_unit_id": "u_p1",
                    "ad_unit_name": "Demo Inter P1",
                    "network_type": "waterfall",
                    "network": "Vungle",
                    "status_bucket": "FAILED_TO_LOAD",
                    "request_pv": "4",
                    "denominator_request_pv": "10",
                    "share": "0.40",
                },
            ],
            configured_units_by_channel={
                ("com.demo.app", "interstitial", "bidding", "AdMob"): {"u_p1", "u_p2"},
                ("com.demo.app", "interstitial", "waterfall", "Vungle"): {"u_p1"},
            },
        )
        combo = payload["combos"]["com.demo.app__interstitial__bidding__Pangle"]
        self.assertEqual(combo["unit_options"][0]["value"], dashboard.ALL_UNIT_OPTION_VALUE)
        all_unit = combo["unit_map"][dashboard.ALL_UNIT_OPTION_VALUE]
        self.assertEqual(all_unit["bidding_status_order"], ["FAILED_TO_LOAD", "AD_LOAD_NOT_ATTEMPTED", "NULL"])
        self.assertEqual(all_unit["waterfall_status_order"], ["AD_LOADED", "FAILED_TO_LOAD", "AD_LOAD_NOT_ATTEMPTED"])
        self.assertAlmostEqual(all_unit["groups"][dashboard.GROUP_A]["denominator_request_pv"], 15.0)
        self.assertAlmostEqual(
            all_unit["network_types"]["bidding"]["rows"][0]["statuses"]["NULL"]["request_pv"],
            3.0,
        )
        self.assertNotIn("NULL", all_unit["network_types"]["waterfall"]["rows"][0]["statuses"])

    def test_winning_type_network_status_page_script_contains_linked_selectors_without_null_toggle(self) -> None:
        script = dashboard.build_winning_type_network_status_page_script()
        self.assertIn("winner-type-select", script)
        self.assertIn("winner-network-select", script)
        self.assertIn("unit-select", script)
        self.assertNotIn("不考虑 NULL", script)
        self.assertIn("ALL UNIT", script)
        self.assertNotIn("AD_LOADED", script)
        self.assertIn("sortState", script)
        self.assertIn("heatStyle", script)
        self.assertIn("const previousUnitValue=unit.value;", script)
        self.assertIn("if(units.includes(previousUnitValue)){unit.value=previousUnitValue;}", script)

    def test_entry_html_links_to_new_winning_type_network_page(self) -> None:
        html = dashboard.build_entry_html()
        self.assertIn("胜利渠道", html)
        self.assertIn("ab_winning_type_network_status_dashboard.html", html)

    def test_runner_registers_new_winning_type_network_sql(self) -> None:
        self.assertIn("winning_type_network_status_hit_rate_by_unit.sql", runner.SQL_TO_CSV)
        self.assertEqual(
            runner.SQL_TO_CSV["winning_type_network_status_hit_rate_by_unit.sql"],
            "winning_type_network_status_hit_rate_by_unit.csv",
        )


if __name__ == "__main__":
    unittest.main()
