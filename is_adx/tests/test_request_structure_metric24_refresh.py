"""请求结构页 metric2 / metric4 新口径回归测试。"""

from __future__ import annotations

import importlib.util
from pathlib import Path
import unittest


REPO_ROOT = Path(__file__).resolve().parents[1]
PROJECT_DIR = REPO_ROOT / "projects" / "ab_dashboard"
SCRIPT_PATH = PROJECT_DIR / "scripts" / "build_ab_share_dashboard.py"

spec = importlib.util.spec_from_file_location("build_ab_share_dashboard", SCRIPT_PATH)
dashboard = importlib.util.module_from_spec(spec)
assert spec is not None and spec.loader is not None
spec.loader.exec_module(dashboard)


class RequestStructureMetric24RefreshTests(unittest.TestCase):
    """覆盖本轮 metric2 / metric4 的展示与数据结构变化。"""

    def test_metric2_uses_bw_bucket_labels(self) -> None:
        payload = dashboard.build_request_structure_metric2(
            [
                {
                    "experiment_group": dashboard.GROUP_A,
                    "product": "demo",
                    "ad_format": "interstitial",
                    "network_cnt": "4",
                    "bidding_cnt": "0",
                    "waterfall_cnt": "4",
                    "request_pv": "2",
                    "denominator_request_pv": "5",
                    "share": "0.4",
                },
                {
                    "experiment_group": dashboard.GROUP_A,
                    "product": "demo",
                    "ad_format": "interstitial",
                    "network_cnt": "4",
                    "bidding_cnt": "1",
                    "waterfall_cnt": "3",
                    "request_pv": "3",
                    "denominator_request_pv": "5",
                    "share": "0.6",
                },
            ],
            [
                {
                    "experiment_group": dashboard.GROUP_A,
                    "product": "demo",
                    "ad_format": "interstitial",
                    "placement_cnt": "6",
                    "bidding_placement_cnt": "2",
                    "waterfall_placement_cnt": "4",
                    "request_pv": "4",
                    "denominator_request_pv": "5",
                    "share": "0.8",
                },
                {
                    "experiment_group": dashboard.GROUP_A,
                    "product": "demo",
                    "ad_format": "interstitial",
                    "placement_cnt": "6",
                    "bidding_placement_cnt": "3",
                    "waterfall_placement_cnt": "3",
                    "request_pv": "1",
                    "denominator_request_pv": "5",
                    "share": "0.2",
                },
            ],
        )

        combo = payload["combos"]["demo__interstitial"]
        network_payload = combo["network_view"]["cnt_map"]["4"]
        placement_payload = combo["placement_view"]["cnt_map"]["6"]
        self.assertEqual(combo["network_view"]["bucket_label"], "B{bidding_cnt}+W{waterfall_cnt}")
        self.assertEqual(combo["placement_view"]["bucket_label"], "B{bidding_placement_cnt}+W{waterfall_placement_cnt}")
        self.assertEqual(network_payload["bucket_options"], ["B0+W4", "B1+W3"])
        self.assertEqual(placement_payload["bucket_options"], ["B2+W4", "B3+W3"])
        self.assertEqual(network_payload["groups"][dashboard.GROUP_A]["points"][0]["bucket_key"], "B0+W4")
        self.assertEqual(
            network_payload["groups"][dashboard.GROUP_A]["points"][0]["axis_label"],
            "B{bidding_cnt}+W{waterfall_cnt}",
        )
        self.assertEqual(
            placement_payload["groups"][dashboard.GROUP_A]["points"][0]["axis_label"],
            "B{bidding_placement_cnt}+W{waterfall_placement_cnt}",
        )

    def test_metric4_builds_count_structure_status_target_distribution(self) -> None:
        payload = dashboard.build_request_structure_metric4(
            [
                {
                    "experiment_group": dashboard.GROUP_A,
                    "product": "demo",
                    "ad_format": "interstitial",
                    "success_scope": "all",
                    "network_cnt": "3",
                    "bidding_cnt": "1",
                    "waterfall_cnt": "2",
                    "status_bucket": "FAILED_TO_LOAD",
                    "network_type": "bidding",
                    "network": "Google",
                    "request_pv": "3",
                    "denominator_request_pv": "10",
                    "share": "0.3",
                },
                {
                    "experiment_group": dashboard.GROUP_B,
                    "product": "demo",
                    "ad_format": "interstitial",
                    "success_scope": "all",
                    "network_cnt": "3",
                    "bidding_cnt": "1",
                    "waterfall_cnt": "2",
                    "status_bucket": "FAILED_TO_LOAD",
                    "network_type": "waterfall",
                    "network": "Google",
                    "request_pv": "4",
                    "denominator_request_pv": "8",
                    "share": "0.5",
                },
                {
                    "experiment_group": dashboard.GROUP_A,
                    "product": "demo",
                    "ad_format": "interstitial",
                    "success_scope": "all",
                    "network_cnt": "3",
                    "bidding_cnt": "1",
                    "waterfall_cnt": "2",
                    "status_bucket": "AD_LOADED",
                    "network_type": "waterfall",
                    "network": "Meta",
                    "request_pv": "2",
                    "denominator_request_pv": "10",
                    "share": "0.2",
                },
            ],
            [
                {
                    "experiment_group": dashboard.GROUP_A,
                    "product": "demo",
                    "ad_format": "interstitial",
                    "success_scope": "all",
                    "placement_cnt": "5",
                    "bidding_placement_cnt": "2",
                    "waterfall_placement_cnt": "3",
                    "status_bucket": "FAILED_TO_LOAD",
                    "placement_id": "p1",
                    "request_pv": "6",
                    "denominator_request_pv": "10",
                    "share": "0.6",
                },
                {
                    "experiment_group": dashboard.GROUP_B,
                    "product": "demo",
                    "ad_format": "interstitial",
                    "success_scope": "all",
                    "placement_cnt": "5",
                    "bidding_placement_cnt": "2",
                    "waterfall_placement_cnt": "3",
                    "status_bucket": "FAILED_TO_LOAD",
                    "placement_id": "p2",
                    "request_pv": "4",
                    "denominator_request_pv": "8",
                    "share": "0.5",
                },
            ],
            include_success_scope=True,
        )

        combo = payload["combos"]["demo__interstitial__all"]
        network_bucket = combo["network_view"]["cnt_map"]["3"]
        placement_bucket = combo["placement_view"]["cnt_map"]["5"]
        network_status = network_bucket["structure_map"]["B1+W2"]["status_map"]["FAILED_TO_LOAD"]
        placement_status = placement_bucket["structure_map"]["B2+W3"]["status_map"]["FAILED_TO_LOAD"]

        self.assertEqual(payload["chart_mode"], "distribution")
        self.assertEqual(network_bucket["structure_options"], ["B1+W2"])
        self.assertEqual(placement_bucket["structure_options"], ["B2+W3"])
        self.assertEqual(set(network_status["target_options"]), {"B-Google", "W-Google"})
        self.assertEqual(placement_status["target_options"], ["p1", "p2"])
        self.assertEqual(
            {point["bucket_key"] for point in network_status["groups"][dashboard.GROUP_A]["points"]},
            {"B-Google", "W-Google"},
        )
        self.assertAlmostEqual(
            next(
                point["share"]
                for point in network_status["groups"][dashboard.GROUP_B]["points"]
                if point["bucket_key"] == "W-Google"
            ),
            0.5,
        )
        self.assertEqual(placement_status["groups"][dashboard.GROUP_A]["points"][0]["bucket_key"], "p1")

    def test_metric4_script_uses_structure_and_status_selectors(self) -> None:
        script = dashboard.build_request_structure_page_script()
        self.assertIn("renderMetric4DistributionBlock", script)
        self.assertIn("B/W 结构（单选）", script)
        self.assertIn("status（单选）", script)
        self.assertIn("network 渠道分布", script)

    def test_request_structure_text_mentions_bw_bucket_label(self) -> None:
        metric2_text = " ".join(dashboard.REQUEST_STRUCTURE_TEXT["metric2"])
        metric4_text = " ".join(dashboard.REQUEST_STRUCTURE_TEXT["metric4"])
        self.assertIn("Bx+Wy", metric2_text)
        self.assertIn("Bx+Wy", metric4_text)
        self.assertIn("status", metric4_text)


if __name__ == "__main__":
    unittest.main()
