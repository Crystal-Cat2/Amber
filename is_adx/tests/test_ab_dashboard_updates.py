"""AB dashboard 双页拆分与交互回归测试。"""

from __future__ import annotations

import importlib.util
from pathlib import Path
from unittest import mock
import unittest


REPO_ROOT = Path(__file__).resolve().parents[1]
PROJECT_DIR = REPO_ROOT / "projects" / "ab_dashboard"
SCRIPT_PATH = PROJECT_DIR / "scripts" / "build_ab_share_dashboard.py"
RUNNER_PATH = PROJECT_DIR / "scripts" / "run_ab_dashboard_sql.py"
MAIN_ENTRY_SCRIPT = PROJECT_DIR / "scripts" / "build_ab_dashboard.py"
HOME_SCRIPT = PROJECT_DIR / "scripts" / "build_ab_dashboard_home.py"
REQUEST_STRUCTURE_SCRIPT = PROJECT_DIR / "scripts" / "build_ab_dashboard_request_structure.py"
COVERAGE_SCRIPT = PROJECT_DIR / "scripts" / "build_ab_dashboard_coverage_analysis.py"
NULL_BIDDING_SCRIPT = PROJECT_DIR / "scripts" / "build_ab_dashboard_null_bidding.py"
BIDDING_STATUS_SCRIPT = PROJECT_DIR / "scripts" / "build_ab_dashboard_bidding_network_status.py"
WINNING_STATUS_SCRIPT = PROJECT_DIR / "scripts" / "build_ab_dashboard_winning_type_network_status.py"
SUCCESS_MAPPING_SCRIPT = PROJECT_DIR / "scripts" / "build_ab_dashboard_success_mapping.py"
SUCCESS_REQUEST_SCRIPT = PROJECT_DIR / "scripts" / "build_ab_dashboard_success_request.py"

spec = importlib.util.spec_from_file_location("build_ab_share_dashboard", SCRIPT_PATH)
dashboard = importlib.util.module_from_spec(spec)
assert spec is not None and spec.loader is not None
spec.loader.exec_module(dashboard)

runner_spec = importlib.util.spec_from_file_location("run_ab_dashboard_sql", RUNNER_PATH)
runner = importlib.util.module_from_spec(runner_spec)
assert runner_spec is not None and runner_spec.loader is not None
runner_spec.loader.exec_module(runner)


class AbDashboardUpdateTests(unittest.TestCase):
    """覆盖拆分页面后的核心行为。"""

    def test_dashboard_project_layout_exists(self) -> None:
        """AB dashboard 应继续位于独立项目目录并产出业务 HTML。"""
        self.assertTrue(PROJECT_DIR.exists())
        self.assertTrue(SCRIPT_PATH.exists())
        self.assertTrue(MAIN_ENTRY_SCRIPT.exists())
        self.assertTrue(HOME_SCRIPT.exists())
        self.assertTrue(REQUEST_STRUCTURE_SCRIPT.exists())
        self.assertTrue(COVERAGE_SCRIPT.exists())
        self.assertTrue(NULL_BIDDING_SCRIPT.exists())
        self.assertTrue(BIDDING_STATUS_SCRIPT.exists())
        self.assertTrue(WINNING_STATUS_SCRIPT.exists())
        self.assertTrue(SUCCESS_MAPPING_SCRIPT.exists())
        self.assertTrue(SUCCESS_REQUEST_SCRIPT.exists())
        self.assertTrue((PROJECT_DIR / "outputs" / "ab_share_dashboard.html").exists())
        self.assertTrue((PROJECT_DIR / "outputs" / "ab_share_dashboard.deploy.html").exists())
        self.assertTrue((PROJECT_DIR / "outputs" / "ab_request_structure_dashboard.html").exists())
        self.assertTrue((PROJECT_DIR / "outputs" / "ab_request_structure_country_dashboard.html").exists())
        self.assertTrue((PROJECT_DIR / "outputs" / "ab_coverage_analysis_dashboard.html").exists())
        self.assertTrue((PROJECT_DIR / "outputs" / "ab_null_bidding_dashboard.html").exists())
        self.assertTrue((PROJECT_DIR / "outputs" / "ab_success_mapping_dashboard.html").exists())
        self.assertTrue((PROJECT_DIR / "outputs" / "ab_success_request_dashboard.html").exists())
        self.assertTrue((PROJECT_DIR / "assets" / "echarts.min.js").exists())

    def test_entry_html_links_to_business_pages(self) -> None:
        """入口页应提供全量业务页面入口，并使用单列一行一个的布局。"""
        html = dashboard.build_entry_html()
        self.assertIn("请求结构分布", html)
        self.assertIn("请求结构分布（Country）", html)
        self.assertIn("请求结构分布（Unit）", html)
        self.assertIn("覆盖率分析", html)
        self.assertIn("Null Bidding", html)
        self.assertIn("Bidding Network Status", html)
        self.assertIn("胜利渠道", html)
        self.assertIn("成功 network / placement 分布", html)
        self.assertIn("成功 Request 分层分析", html)
        self.assertIn("ab_request_structure_dashboard.html", html)
        self.assertIn("ab_request_structure_country_dashboard.html", html)
        self.assertIn("ab_request_structure_unit_dashboard.html", html)
        self.assertIn("ab_coverage_analysis_dashboard.html", html)
        self.assertIn("ab_null_bidding_dashboard.html", html)
        self.assertIn("ab_bidding_network_status_dashboard.html", html)
        self.assertIn("ab_winning_type_network_status_dashboard.html", html)
        self.assertIn("ab_success_mapping_dashboard.html", html)
        self.assertIn("ab_success_request_dashboard.html", html)
        self.assertIn("entry-list", html)
        self.assertIn("entry-row", html)
        self.assertNotIn("grid-template-columns:repeat(auto-fit", html)

    def test_entry_generation_writes_local_and_deploy_home_pages(self) -> None:
        """主入口生成时应同时写本地入口页和 deploy 入口页。"""
        captured_paths: list[Path] = []

        def fake_write(path: Path, html: str, required_strings: list[str]) -> None:
            del html, required_strings
            captured_paths.append(path)

        with mock.patch.object(dashboard, "write_validated_html", side_effect=fake_write):
            dashboard.write_dashboards({"entry"})

        self.assertIn(PROJECT_DIR / "outputs" / "ab_share_dashboard.html", captured_paths)
        self.assertIn(PROJECT_DIR / "outputs" / "ab_share_dashboard.deploy.html", captured_paths)

    def test_request_structure_html_contains_old_hover_logic(self) -> None:
        """请求结构分布页应恢复 axis tooltip 与鼠标纵向跟手逻辑。"""
        html = dashboard.build_dashboard_html(
            page_title="请求结构分布",
            metrics={
                "metric1": {"title": "旧1", "desc": dashboard.REQUEST_STRUCTURE_TEXT["metric1"], "combos": {}, "series_keys": []},
                "metric2": {"title": "旧2", "desc": dashboard.REQUEST_STRUCTURE_TEXT["metric2"], "combos": {}, "series_keys": []},
                "metric3": {"title": "旧3", "desc": dashboard.REQUEST_STRUCTURE_TEXT["metric3"], "combos": {}, "series_keys": []},
            },
            products=["demo"],
            ad_formats=["interstitial"],
            page_key="request_structure",
        )
        self.assertIn("trigger:'axis'", html)
        self.assertIn("window._current_mouse_y", html)
        self.assertIn("chart.getZr().on('mousemove'", html)
        self.assertNotIn("trigger:'item'", html)
        self.assertIn(">返回首页<", html)
        self.assertIn('href="ab_share_dashboard.html"', html)

    def test_coverage_html_contains_same_hover_logic(self) -> None:
        """覆盖率分析页应与请求结构分布页使用同一套 tooltip 跟手交互。"""
        html = dashboard.build_dashboard_html(
            page_title="覆盖率分析",
            metrics={
                "metric1": {"title": "新1", "desc": dashboard.COVERAGE_TEXT["metric1"], "combos": {}, "series_keys": []},
                "metric2": {"title": "新2", "desc": dashboard.COVERAGE_TEXT["metric2"], "combos": {}, "series_keys": [], "chart_mode": "coverage"},
                "metric3": {"title": "新3", "desc": dashboard.COVERAGE_TEXT["metric3"], "combos": {}, "series_keys": [], "chart_mode": "coverage"},
            },
            products=["demo"],
            ad_formats=["interstitial"],
            page_key="coverage_analysis",
        )
        self.assertIn("trigger:'axis'", html)
        self.assertIn("window._current_mouse_y", html)
        self.assertIn("chart.getZr().on('mousemove'", html)
        self.assertNotIn("trigger:'item'", html)
        self.assertIn(">返回首页<", html)
        self.assertIn('href="ab_share_dashboard.html"', html)

    def test_specialized_pages_include_home_button(self) -> None:
        """非通用业务页也应提供固定返回首页按钮。"""
        null_html = dashboard.build_null_bidding_html(
            {
                "title": "Null Bidding Unit Share",
                "desc": ["A组 = no_is_adx"],
                "format_order": [],
                "status_options": [],
                "platform_order": [],
                "groups": dashboard.GROUP_LABELS,
                "platforms": {},
            }
        )
        success_html = dashboard.build_success_mapping_html(
            {
                "title": "成功 network / placement 分布",
                "desc": ["统计时间范围：2026-01-05 到 2026-01-12。"],
                "groups": dashboard.GROUP_LABELS,
                "products": [],
                "ad_formats": [],
                "metrics": {},
            }
        )
        self.assertIn(">返回首页<", null_html)
        self.assertIn('href="ab_share_dashboard.html"', null_html)
        self.assertIn(">返回首页<", success_html)
        self.assertIn('href="ab_share_dashboard.html"', success_html)

    def test_success_request_wrapper_includes_home_button(self) -> None:
        """成功 request 页包装后应带返回首页按钮，且允许自定义 deploy 首页链接。"""
        html = dashboard.build_success_request_html(home_href="http://deploy-entry/")
        self.assertIn("成功 Request 分层分析", html)
        self.assertIn(">返回首页<", html)
        self.assertIn('href="http://deploy-entry/"', html)
        self.assertIn("payload.cntRows", html)

    def test_build_dashboard_html_resolves_default_page_title_from_page_key(self) -> None:
        """dashboard HTML 应优先从 page_key 内部派生中文标题，避免外部 shell 传中文。"""
        html = dashboard.build_dashboard_html(
            metrics={},
            products=["demo"],
            ad_formats=["interstitial"],
            page_key="request_structure",
        )
        self.assertIn("<title>请求结构分布</title>", html)
        self.assertIn("<h1>请求结构分布</h1>", html)

    def test_validate_generated_html_text_rejects_corrupted_chinese(self) -> None:
        """最终生成的 HTML 若出现 ??? 或 replacement char，应直接判失败。"""
        with self.assertRaises(ValueError):
            dashboard.validate_generated_html_text(
                "<title>???</title><h1>请求结构分布</h1>",
                "demo.html",
                required_strings=["请求结构分布"],
            )
        with self.assertRaises(ValueError):
            dashboard.validate_generated_html_text(
                "<title>请求结构分布</title><h1>请求结构分布�</h1>",
                "demo.html",
                required_strings=["请求结构分布"],
            )
        dashboard.validate_generated_html_text(
            "<title>请求结构分布</title><h1>请求结构分布</h1><p>统计时间范围</p>",
            "demo.html",
            required_strings=["请求结构分布", "统计时间范围"],
        )

    def test_write_dashboards_only_skips_unselected_payload_builds(self) -> None:
        """ASCII 的 --only/only_pages 入口应跳过无关页面的 payload 计算。"""
        with mock.patch.object(dashboard, "build_request_structure_payload", return_value={"metrics": {}, "products": [], "ad_formats": [], "success_scopes": []}) as request_mock:
            with mock.patch.object(dashboard, "build_coverage_analysis_payload", side_effect=AssertionError("coverage payload should not be built")):
                with mock.patch.object(dashboard, "write_validated_html"):
                    dashboard.write_dashboards({"request_structure"})
        request_mock.assert_called_once()

    def test_write_dashboards_can_generate_success_request_wrapper(self) -> None:
        """成功 request 页应纳入统一 write_dashboards 输出集合。"""
        captured_paths: list[Path] = []

        def fake_write(path: Path, html: str, required_strings: list[str]) -> None:
            del html, required_strings
            captured_paths.append(path)

        with mock.patch.object(dashboard, "write_validated_html", side_effect=fake_write):
            outputs = dashboard.write_dashboards({"success_request"})

        self.assertIn(PROJECT_DIR / "outputs" / "ab_success_request_dashboard.html", captured_paths)
        self.assertIn(PROJECT_DIR / "outputs" / "ab_success_request_dashboard.html", outputs.values())

    def test_request_structure_metric1_uses_total_request_network_cnt_distribution(self) -> None:
        """请求结构分布页的 metric1 应同时产出 network 与 placement 两套分布。"""
        payload = dashboard.build_request_structure_metric1(
            [
                {
                    "experiment_group": dashboard.GROUP_A,
                    "product": "demo",
                    "ad_format": "interstitial",
                    "network_cnt": "1",
                    "request_pv": "4",
                    "denominator_request_pv": "10",
                    "share": "0.4",
                },
                {
                    "experiment_group": dashboard.GROUP_A,
                    "product": "demo",
                    "ad_format": "interstitial",
                    "network_cnt": "3",
                    "request_pv": "6",
                    "denominator_request_pv": "10",
                    "share": "0.6",
                },
                {
                    "experiment_group": dashboard.GROUP_B,
                    "product": "demo",
                    "ad_format": "interstitial",
                    "network_cnt": "1",
                    "request_pv": "2",
                    "denominator_request_pv": "8",
                    "share": "0.25",
                },
            ],
            [
                {
                    "experiment_group": dashboard.GROUP_A,
                    "product": "demo",
                    "ad_format": "interstitial",
                    "placement_cnt": "2",
                    "request_pv": "7",
                    "denominator_request_pv": "10",
                    "share": "0.7",
                },
                {
                    "experiment_group": dashboard.GROUP_B,
                    "product": "demo",
                    "ad_format": "interstitial",
                    "placement_cnt": "3",
                    "request_pv": "4",
                    "denominator_request_pv": "8",
                    "share": "0.5",
                },
            ],
            [],
        )
        combo = payload["combos"]["demo__interstitial"]
        network_points = combo["network_view"]["groups"][dashboard.GROUP_A]["points"]
        placement_points = combo["placement_view"]["groups"][dashboard.GROUP_A]["points"]
        self.assertEqual(combo["network_view"]["count_options"], ["1", "3"])
        self.assertEqual(combo["placement_view"]["count_options"], ["2", "3"])
        self.assertEqual(network_points[0]["bucket_key"], "1")
        self.assertAlmostEqual(network_points[0]["share"], 0.4)
        self.assertAlmostEqual(network_points[0]["denominator_request_pv"], 10.0)
        self.assertEqual(network_points[1]["bucket_key"], "3")
        self.assertAlmostEqual(network_points[1]["request_pv"], 6.0)
        self.assertEqual(placement_points[0]["bucket_key"], "2")
        self.assertAlmostEqual(placement_points[0]["share"], 0.7)

    def test_request_structure_metric1_uses_fixed_1_to_35_buckets_with_35_plus_tail(self) -> None:
        """metric1 的 placement 视图应固定展示 1-35，并把更大的桶合并到 35+。"""
        payload = dashboard.build_request_structure_metric1(
            [
                {
                    "experiment_group": dashboard.GROUP_A,
                    "product": "demo",
                    "ad_format": "rewarded",
                    "network_cnt": "1",
                    "request_pv": "10",
                    "denominator_request_pv": "20",
                    "share": "0.5",
                }
            ],
            [
                {
                    "experiment_group": dashboard.GROUP_A,
                    "product": "demo",
                    "ad_format": "rewarded",
                    "placement_cnt": "6",
                    "request_pv": "3",
                    "denominator_request_pv": "10",
                    "share": "0.3",
                },
                {
                    "experiment_group": dashboard.GROUP_A,
                    "product": "demo",
                    "ad_format": "rewarded",
                    "placement_cnt": "35",
                    "request_pv": "4",
                    "denominator_request_pv": "10",
                    "share": "0.4",
                },
                {
                    "experiment_group": dashboard.GROUP_A,
                    "product": "demo",
                    "ad_format": "rewarded",
                    "placement_cnt": "36",
                    "request_pv": "1",
                    "denominator_request_pv": "10",
                    "share": "0.1",
                },
            ],
            [],
        )

        combo = payload["combos"]["demo__rewarded"]
        placement_view = combo["placement_view"]
        placement_points = placement_view["groups"][dashboard.GROUP_A]["points"]

        self.assertEqual(placement_view["count_options"][:6], ["1", "2", "3", "4", "5", "6"])
        self.assertEqual(placement_view["count_options"][-2:], ["35", "35+"])
        self.assertEqual(len(placement_view["count_options"]), 36)
        self.assertEqual(placement_points[5]["bucket_key"], "6")
        self.assertAlmostEqual(placement_points[5]["request_pv"], 3.0)
        self.assertEqual(placement_points[34]["bucket_key"], "35")
        self.assertAlmostEqual(placement_points[34]["request_pv"], 4.0)
        self.assertEqual(placement_points[35]["bucket_key"], "35+")
        self.assertAlmostEqual(placement_points[35]["request_pv"], 1.0)

    def test_request_structure_metric1_uses_shared_dynamic_axis_max(self) -> None:
        """metric1 应按当前 network/placement 最高 share 动态收紧共享纵轴。"""
        payload = dashboard.build_request_structure_metric1(
            [
                {
                    "experiment_group": dashboard.GROUP_A,
                    "product": "demo",
                    "ad_format": "interstitial",
                    "network_cnt": "1",
                    "request_pv": "47",
                    "denominator_request_pv": "100",
                    "share": "0.47",
                }
            ],
            [
                {
                    "experiment_group": dashboard.GROUP_B,
                    "product": "demo",
                    "ad_format": "interstitial",
                    "placement_cnt": "2",
                    "request_pv": "42",
                    "denominator_request_pv": "100",
                    "share": "0.42",
                }
            ],
            [],
        )

        combo = payload["combos"]["demo__interstitial"]
        self.assertAlmostEqual(combo["axis_max"], 0.55)

    def test_request_structure_metric1_builds_rank_block_for_network_and_placement(self) -> None:
        """metric1 应删除旧第三图，并新增 network/placement 两块 success_rank 下钻。"""
        payload = dashboard.build_request_structure_metric1(
            [
                {
                    "experiment_group": dashboard.GROUP_A,
                    "product": "demo",
                    "ad_format": "interstitial",
                    "network_cnt": "4",
                    "request_pv": "7",
                    "denominator_request_pv": "10",
                    "share": "0.7",
                }
            ],
            [
                {
                    "experiment_group": dashboard.GROUP_A,
                    "product": "demo",
                    "ad_format": "interstitial",
                    "placement_cnt": "6",
                    "request_pv": "7",
                    "denominator_request_pv": "10",
                    "share": "0.7",
                }
            ],
            [
                {
                    "experiment_group": dashboard.GROUP_A,
                    "product": "demo",
                    "ad_format": "interstitial",
                    "cnt_type": "network",
                    "cnt_value": "4",
                    "success_rank": "2",
                    "request_pv": "3",
                    "bucket_success_request_pv": "6",
                    "bucket_total_request_pv": "10",
                },
                {
                    "experiment_group": dashboard.GROUP_B,
                    "product": "demo",
                    "ad_format": "interstitial",
                    "cnt_type": "network",
                    "cnt_value": "4",
                    "success_rank": "3",
                    "request_pv": "4",
                    "bucket_success_request_pv": "5",
                    "bucket_total_request_pv": "8",
                },
                {
                    "experiment_group": dashboard.GROUP_A,
                    "product": "demo",
                    "ad_format": "interstitial",
                    "cnt_type": "placement",
                    "cnt_value": "6",
                    "success_rank": "2",
                    "request_pv": "2",
                    "bucket_success_request_pv": "4",
                    "bucket_total_request_pv": "7",
                },
            ],
        )

        combo = payload["combos"]["demo__interstitial"]
        self.assertNotIn("placement_success_failed_view", combo)
        rank_block = combo["rank_block"]
        network_rank_view = rank_block["network_rank_view"]
        placement_rank_view = rank_block["placement_rank_view"]
        self.assertEqual(network_rank_view["cnt_type"], "network")
        self.assertEqual(network_rank_view["cnt_options"], ["4"])
        self.assertEqual(network_rank_view["default_cnt"], "4")
        self.assertEqual(
            network_rank_view["bucket_map"]["4"]["groups"][dashboard.GROUP_A]["points"][1]["bucket_key"],
            "2",
        )
        self.assertAlmostEqual(
            network_rank_view["bucket_map"]["4"]["groups"][dashboard.GROUP_A]["summary"]["success_rate"],
            0.6,
        )
        self.assertEqual(placement_rank_view["cnt_options"], ["6"])
        self.assertAlmostEqual(
            placement_rank_view["bucket_map"]["6"]["groups"][dashboard.GROUP_A]["summary"]["success_rate"],
            4 / 7,
        )
        self.assertAlmostEqual(combo["axis_max"], 0.8)

    def test_request_structure_metric_builders_support_country_combo_keys(self) -> None:
        """country 版请求结构应把 country 纳入 combo key，并保留到 payload。"""
        payload = dashboard.build_request_structure_metric1(
            [
                {
                    "experiment_group": dashboard.GROUP_A,
                    "product": "demo",
                    "ad_format": "interstitial",
                    "country": "US",
                    "network_cnt": "4",
                    "request_pv": "7",
                    "denominator_request_pv": "10",
                    "share": "0.7",
                }
            ],
            [
                {
                    "experiment_group": dashboard.GROUP_B,
                    "product": "demo",
                    "ad_format": "interstitial",
                    "country": "US",
                    "placement_cnt": "6",
                    "request_pv": "4",
                    "denominator_request_pv": "8",
                    "share": "0.5",
                }
            ],
            [],
        )

        combo = payload["combos"]["demo__interstitial__US"]
        self.assertEqual(combo["product"], "demo")
        self.assertEqual(combo["ad_format"], "interstitial")
        self.assertEqual(combo["country"], "US")

    def test_request_structure_metric2_builds_bidding_waterfall_mix_buckets(self) -> None:
        """请求结构分布页的 metric2 应同时支持 network 与 placement 组合桶。"""
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
                    "pv_count": "3",
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
            ]
        )
        combo = payload["combos"]["demo__interstitial"]
        network_payload = combo["network_view"]["cnt_map"]["4"]
        placement_payload = combo["placement_view"]["cnt_map"]["6"]
        network_points = network_payload["groups"][dashboard.GROUP_A]["points"]
        placement_points = placement_payload["groups"][dashboard.GROUP_A]["points"]
        self.assertEqual(combo["network_view"]["count_options"], ["4"])
        self.assertEqual(combo["placement_view"]["count_options"], ["6"])
        self.assertEqual(network_payload["bucket_options"], ["B0+W4", "B1+W3"])
        self.assertEqual(placement_payload["bucket_options"], ["B2+W4", "B3+W3"])
        self.assertEqual(network_points[0]["bucket_key"], "B0+W4")
        self.assertAlmostEqual(network_points[0]["share"], 0.4)
        self.assertAlmostEqual(network_points[1]["request_pv"], 3.0)
        self.assertAlmostEqual(placement_points[0]["share"], 0.8)
        self.assertAlmostEqual(placement_points[1]["denominator_request_pv"], 5.0)

    def test_request_structure_metric3_builds_status_distribution_under_type_bucket(self) -> None:
        """请求结构分布页的 metric3 应同时支持 network 与 placement 的 status 下钻。"""
        payload = dashboard.build_request_structure_metric3(
            [
                {
                    "experiment_group": dashboard.GROUP_A,
                    "product": "demo",
                    "ad_format": "interstitial",
                    "network_cnt": "4",
                    "network_type": "waterfall",
                    "type_network_cnt": "3",
                    "status_bucket": "AD_LOADED",
                    "request_pv": "2",
                    "denominator_request_pv": "5",
                    "share": "0.4",
                },
                {
                    "experiment_group": dashboard.GROUP_A,
                    "product": "demo",
                    "ad_format": "interstitial",
                    "network_cnt": "4",
                    "network_type": "waterfall",
                    "type_network_cnt": "3",
                    "status_bucket": "FAILED_TO_LOAD",
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
                    "network_type": "waterfall",
                    "type_placement_cnt": "4",
                    "status_bucket": "AD_LOADED",
                    "request_pv": "1",
                    "denominator_request_pv": "5",
                    "share": "0.2",
                },
                {
                    "experiment_group": dashboard.GROUP_A,
                    "product": "demo",
                    "ad_format": "interstitial",
                    "placement_cnt": "6",
                    "network_type": "waterfall",
                    "type_placement_cnt": "4",
                    "status_bucket": "FAILED_TO_LOAD",
                    "request_pv": "4",
                    "denominator_request_pv": "5",
                    "share": "0.8",
                },
            ]
        )
        combo = payload["combos"]["demo__interstitial"]
        network_payload = combo["network_view"]["cnt_map"]["4"]["type_map"]["waterfall"]["type_cnt_map"]["3"]
        placement_payload = combo["placement_view"]["cnt_map"]["6"]["type_map"]["waterfall"]["type_cnt_map"]["4"]
        network_points = network_payload["groups"][dashboard.GROUP_A]["points"]
        placement_points = placement_payload["groups"][dashboard.GROUP_A]["points"]
        self.assertEqual(payload["chart_mode"], "distribution")
        self.assertEqual(combo["network_view"]["count_options"], ["4"])
        self.assertEqual(combo["placement_view"]["count_options"], ["6"])
        self.assertEqual(combo["network_view"]["cnt_map"]["4"]["network_type_options"], ["waterfall"])
        self.assertEqual(combo["placement_view"]["cnt_map"]["6"]["type_map"]["waterfall"]["type_count_options"], ["4"])
        self.assertEqual(network_payload["status_options"], ["AD_LOADED", "FAILED_TO_LOAD"])
        self.assertEqual(placement_payload["status_options"], ["AD_LOADED", "FAILED_TO_LOAD"])
        self.assertEqual(network_points[0]["bucket_key"], "AD_LOADED")
        self.assertAlmostEqual(network_points[0]["share"], 0.4)
        self.assertAlmostEqual(network_points[1]["request_pv"], 3.0)
        self.assertAlmostEqual(placement_points[1]["share"], 0.8)

    def test_request_structure_metric4_builds_cnt_level_network_and_placement_tables(self) -> None:
        """请求结构页新增 metric4 应按 cnt 下钻出 network 与 placement 两块图表。"""
        payload = dashboard.build_request_structure_metric4(
            [
                {
                    "experiment_group": dashboard.GROUP_A,
                    "product": "demo",
                    "ad_format": "interstitial",
                    "success_scope": "all",
                    "network_cnt": "3",
                    "bidding_cnt": "2",
                    "waterfall_cnt": "1",
                    "status_bucket": "AD_LOADED",
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
                    "bidding_cnt": "2",
                    "waterfall_cnt": "1",
                    "status_bucket": "AD_LOADED",
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
                    "bidding_cnt": "2",
                    "waterfall_cnt": "1",
                    "status_bucket": "FAILED_TO_LOAD",
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
                    "status_bucket": "AD_LOAD_NOT_ATTEMPTED",
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
                    "status_bucket": "AD_LOAD_NOT_ATTEMPTED",
                    "placement_id": "p2",
                    "request_pv": "4",
                    "denominator_request_pv": "8",
                    "share": "0.5",
                },
            ],
            include_success_scope=True,
        )
        combo = payload["combos"]["demo__interstitial__all"]
        network_view = combo["network_view"]
        placement_view = combo["placement_view"]
        
        network_structure_map = network_view["cnt_map"]["3"]["structure_map"]["B2+W1"]
        placement_structure_map = placement_view["cnt_map"]["5"]["structure_map"]["B2+W3"]
        network_loaded = network_structure_map["status_map"]["AD_LOADED"]
        placement_not_attempted = placement_structure_map["status_map"]["AD_LOAD_NOT_ATTEMPTED"]
        
        self.assertEqual(payload["chart_mode"], "distribution")
        self.assertEqual(combo["success_scope"], "all")
        self.assertEqual(network_view["count_options"], ["3"])
        self.assertEqual(placement_view["count_options"], ["5"])
        self.assertEqual(network_structure_map["status_options"], ["AD_LOADED", "FAILED_TO_LOAD"])
        
        self.assertEqual(network_loaded["target_options"], ["W-Google", "B-Google"])
        self.assertAlmostEqual(network_loaded["groups"][dashboard.GROUP_B]["points"][0]["share"], 0.5)
        self.assertEqual(network_loaded["groups"][dashboard.GROUP_B]["points"][0]["bucket_key"], "W-Google")
        
        self.assertEqual(placement_not_attempted["target_options"], ["p1", "p2"])
        self.assertAlmostEqual(placement_not_attempted["groups"][dashboard.GROUP_A]["points"][0]["share"], 0.6)

    def test_request_structure_page_script_renders_metric4_stacked_tables(self) -> None:
        """请求结构页脚本应先渲染新 metric4 的上下两块 cnt 下钻表格。"""
        script = dashboard.build_request_structure_page_script()
        self.assertIn("function renderMetric4(root,c)", script)
        self.assertIn("metricConfig('metric4')", script)
        self.assertIn("if(c4)renderMetric4(root,c4)", script)
        self.assertIn("renderMetric4DistributionBlock(panel,'network 渠道分布',c.network_view,'network_cnt（单选）'", script)
        self.assertIn("renderMetric4DistributionBlock(panel,'placement 分布',c.placement_view,'placement_cnt（单选）'", script)
        self.assertIn("B/W 结构（单选）", script)
        self.assertIn("status（单选）", script)
        self.assertIn("chart-scroll", script)

    def test_request_structure_metric5_builds_network_status_table(self) -> None:
        """请求结构页新的 metric5 应沿用旧 status + network 透视表。"""
        payload = dashboard.build_request_structure_metric5_table(
            [
                {
                    "experiment_group": dashboard.GROUP_A,
                    "product": "demo",
                    "ad_format": "interstitial",
                    "status": "FAILED_TO_LOAD",
                    "network": "Google",
                    "request_pv": "3",
                    "denominator_request_pv": "10",
                    "share": "0.3",
                },
                {
                    "experiment_group": dashboard.GROUP_B,
                    "product": "demo",
                    "ad_format": "interstitial",
                    "status": "FAILED_TO_LOAD",
                    "network": "Google",
                    "request_pv": "4",
                    "denominator_request_pv": "8",
                    "share": "0.5",
                },
                {
                    "experiment_group": dashboard.GROUP_A,
                    "product": "demo",
                    "ad_format": "interstitial",
                    "status": "AD_LOAD_NOT_ATTEMPTED",
                    "network": "Pangle",
                    "request_pv": "2",
                    "denominator_request_pv": "10",
                    "share": "0.2",
                },
            ]
        )
        combo = payload["combos"]["demo__interstitial"]
        row = combo["rows"][0]
        self.assertEqual(payload["table_mode"], "pivot")
        self.assertEqual(combo["groups"][dashboard.GROUP_A]["denominator_request_pv"], 10.0)
        self.assertEqual(combo["groups"][dashboard.GROUP_B]["denominator_request_pv"], 8.0)
        self.assertEqual(row["status"], "FAILED_TO_LOAD")
        self.assertEqual(row["network"], "Google")
        self.assertAlmostEqual(row["groups"][dashboard.GROUP_A]["share"], 0.3)
        self.assertAlmostEqual(row["groups"][dashboard.GROUP_B]["share"], 0.5)

    def test_request_structure_metric6_builds_type_network_status_table(self) -> None:
        """请求结构页新的 metric6 应按 type + network 构建四状态矩阵。"""
        payload = dashboard.build_request_structure_metric6_table(
            [
                {
                    "experiment_group": dashboard.GROUP_A,
                    "product": "demo",
                    "ad_format": "interstitial",
                    "network_type": "bidding",
                    "network": "Google",
                    "status_bucket": "AD_LOADED",
                    "request_pv": "3",
                    "denominator_request_pv": "10",
                    "share": "0.3",
                },
                {
                    "experiment_group": dashboard.GROUP_A,
                    "product": "demo",
                    "ad_format": "interstitial",
                    "network_type": "bidding",
                    "network": "Google",
                    "status_bucket": "NULL",
                    "request_pv": "7",
                    "denominator_request_pv": "10",
                    "share": "0.7",
                },
                {
                    "experiment_group": dashboard.GROUP_B,
                    "product": "demo",
                    "ad_format": "interstitial",
                    "network_type": "bidding",
                    "network": "Google",
                    "status_bucket": "FAILED_TO_LOAD",
                    "request_pv": "2",
                    "denominator_request_pv": "8",
                    "share": "0.25",
                },
                {
                    "experiment_group": dashboard.GROUP_B,
                    "product": "demo",
                    "ad_format": "interstitial",
                    "network_type": "bidding",
                    "network": "Google",
                    "status_bucket": "NULL",
                    "request_pv": "6",
                    "denominator_request_pv": "8",
                    "share": "0.75",
                },
            ]
        )
        combo = payload["combos"]["demo__interstitial"]
        row = combo["rows"][0]
        self.assertEqual(payload["table_mode"], "matrix")
        self.assertEqual(row["network_type"], "bidding")
        self.assertEqual(row["network"], "Google")
        self.assertEqual(combo["groups"][dashboard.GROUP_A]["denominator_request_pv"], 10.0)
        self.assertAlmostEqual(row["groups"][dashboard.GROUP_A]["statuses"]["AD_LOADED"]["share"], 0.3)
        self.assertAlmostEqual(row["groups"][dashboard.GROUP_A]["statuses"]["NULL"]["share"], 0.7)
        self.assertAlmostEqual(row["groups"][dashboard.GROUP_B]["statuses"]["FAILED_TO_LOAD"]["share"], 0.25)
        self.assertAlmostEqual(row["groups"][dashboard.GROUP_B]["statuses"]["NULL"]["share"], 0.75)

    def test_request_structure_page_script_renders_metric6_table(self) -> None:
        """请求结构页脚本应在页尾渲染 type + network 四状态矩阵。"""
        script = dashboard.build_request_structure_page_script()
        self.assertIn("function renderMetric6(root,c)", script)
        self.assertIn("metricConfig('metric6')", script)
        self.assertIn("if(c6)renderMetric6(root,c6)", script)
        self.assertIn("pv", script)
        self.assertIn("是否考虑 NULL（单选）", script)
        self.assertIn("考虑 NULL", script)
        self.assertIn("不考虑 NULL", script)
        self.assertIn("status!=='NULL'", script)
        self.assertIn("nullMode==='exclude_null'", script)
        self.assertNotIn('<div class="detail-top"><h4>type + network 四状态总占比</h4></div>', script)

    def test_request_structure_page_script_keeps_english_status_labels_for_metric_tables(self) -> None:
        """请求结构页的 metric5/6 应继续使用英文状态名。"""
        script = dashboard.build_request_structure_page_script()
        self.assertNotIn("const STATUS_SHORT", script)
        self.assertIn('title="${row.status}"', script)
        self.assertIn('title="${status}"', script)
        self.assertNotIn("状态映射：成功 / 失败 / 未尝试 / 空缺", script)

    def test_request_structure_html_includes_heatmap_and_sticky_table_styles(self) -> None:
        """请求结构页应为 metric4/5/6 提供色阶和 sticky 表头/首列样式。"""
        html = dashboard.build_dashboard_html(
            page_title="请求结构分布",
            metrics={
                "metric1": {"title": "旧1", "desc": dashboard.REQUEST_STRUCTURE_TEXT["metric1"], "combos": {}, "series_keys": []},
                "metric2": {"title": "旧2", "desc": dashboard.REQUEST_STRUCTURE_TEXT["metric2"], "combos": {}, "series_keys": []},
                "metric3": {"title": "旧3", "desc": dashboard.REQUEST_STRUCTURE_TEXT["metric3"], "combos": {}, "series_keys": []},
                "metric4": {"title": "旧4", "desc": dashboard.REQUEST_STRUCTURE_TEXT["metric4"], "combos": {}, "chart_mode": "table"},
                "metric5": {"title": "旧5", "desc": dashboard.REQUEST_STRUCTURE_TEXT["metric5"], "combos": {}, "table_mode": "pivot"},
                "metric6": {"title": "旧6", "desc": dashboard.REQUEST_STRUCTURE_TEXT["metric6"], "combos": {}, "table_mode": "matrix"},
            },
            products=["demo"],
            ad_formats=["interstitial"],
            page_key="request_structure",
        )
        self.assertIn(".heat-cell", html)
        self.assertIn(".metric4-table", html)
        self.assertIn(".metric5-table", html)
        self.assertIn(".sticky-col", html)
        self.assertIn("position:sticky", html)
        self.assertIn("width:100%", html)
        self.assertIn(".explain li", html)
        self.assertIn("overflow-wrap:anywhere", html)
        self.assertIn("const alpha=0.16+norm*0.34", html)

    def test_request_structure_text_is_rewritten_with_pv_share_and_denominator(self) -> None:
        """请求结构页解释文案应改成短句，并明确对象、分子、分母。"""
        self.assertLessEqual(len(dashboard.REQUEST_STRUCTURE_TEXT["metric1"]), 4)
        self.assertLessEqual(len(dashboard.REQUEST_STRUCTURE_TEXT["metric2"]), 4)
        self.assertLessEqual(len(dashboard.REQUEST_STRUCTURE_TEXT["metric3"]), 4)
        metric1_text = " ".join(dashboard.REQUEST_STRUCTURE_TEXT["metric1"])
        metric2_text = " ".join(dashboard.REQUEST_STRUCTURE_TEXT["metric2"])
        metric3_text = " ".join(dashboard.REQUEST_STRUCTURE_TEXT["metric3"])
        metric4_text = " ".join(dashboard.REQUEST_STRUCTURE_TEXT["metric4"])
        metric5_text = " ".join(dashboard.REQUEST_STRUCTURE_TEXT["metric5"])
        metric6_text = " ".join(dashboard.REQUEST_STRUCTURE_TEXT["metric6"])
        self.assertIn("pv", metric1_text)
        self.assertIn("share", metric1_text)
        self.assertIn("分母", metric1_text)
        self.assertIn("placement_cnt", metric1_text)
        self.assertIn("Bx+Wy", metric2_text)
        self.assertIn("B=bidding", metric2_text)
        self.assertIn("W=waterfall", metric2_text)
        self.assertNotIn("B{bidding_cnt}+W{waterfall_cnt}", metric2_text)
        self.assertNotIn("B{bidding_placement_cnt}+W{waterfall_placement_cnt}", metric2_text)
        self.assertIn("pv", metric2_text)
        self.assertIn("分母", metric2_text)
        self.assertIn("type_network_cnt", metric3_text)
        self.assertIn("type_placement_cnt", metric3_text)
        self.assertIn("share", metric3_text)
        self.assertIn("分母", metric3_text)
        self.assertIn("统计对象", metric4_text)
        self.assertIn("分子", metric4_text)
        self.assertIn("分母", metric4_text)
        self.assertIn("分母", metric5_text)
        self.assertIn("NULL", metric6_text)
        self.assertIn("多状态", metric6_text)
        self.assertIn("分母", metric6_text)

    def test_request_structure_text_calls_out_dual_granularity(self) -> None:
        """请求结构页前 3 个指标应同时说明 network 与 placement 的计数方式。"""
        metric1_text = " ".join(dashboard.REQUEST_STRUCTURE_TEXT["metric1"])
        metric2_text = " ".join(dashboard.REQUEST_STRUCTURE_TEXT["metric2"])
        metric3_text = " ".join(dashboard.REQUEST_STRUCTURE_TEXT["metric3"])
        self.assertIn("type + network", metric1_text)
        self.assertIn("placement", metric1_text)
        self.assertIn("不去重", metric1_text)
        self.assertIn("type + network", metric2_text)
        self.assertIn("placement", metric2_text)
        self.assertIn("type_placement_cnt", metric3_text)
        self.assertIn("不做优先级归并", metric3_text)
        self.assertNotIn("左侧", metric2_text)
        self.assertNotIn("右侧", metric2_text)
        self.assertNotIn("左右两侧 selector 独立", metric2_text)

    def test_request_structure_sql_keeps_network_and_adds_placement_variants(self) -> None:
        """请求结构页前 3 个 SQL 应保留 network 口径，并新增 placement 口径。"""
        metric1_sql = (PROJECT_DIR / "sql" / "metric1_total_network_cnt_distribution.sql").read_text(encoding="utf-8")
        metric2_sql = (PROJECT_DIR / "sql" / "metric2_type_mix_distribution.sql").read_text(encoding="utf-8")
        metric3_sql = (PROJECT_DIR / "sql" / "metric3_type_status_distribution.sql").read_text(encoding="utf-8")
        metric1_placement_sql = (PROJECT_DIR / "sql" / "metric1_total_placement_cnt_distribution.sql").read_text(encoding="utf-8")
        metric2_placement_sql = (PROJECT_DIR / "sql" / "metric2_type_placement_mix_distribution.sql").read_text(encoding="utf-8")
        metric3_placement_sql = (PROJECT_DIR / "sql" / "metric3_type_placement_status_distribution.sql").read_text(encoding="utf-8")

        self.assertIn("LOWER(COALESCE(network_type, '')) IN ('bidding', 'waterfall')", metric1_sql)
        self.assertIn("TO_JSON_STRING(STRUCT(network_type, network))", metric1_sql)
        self.assertNotIn("COUNT(DISTINCT STRUCT(", metric1_sql)
        self.assertIn("COUNT(DISTINCT TO_JSON_STRING(STRUCT(network_type, network))) AS network_cnt", metric2_sql)
        self.assertIn("COUNT(DISTINCT IF(network_type = 'bidding', TO_JSON_STRING(STRUCT(network_type, network)), NULL)) AS bidding_cnt", metric2_sql)
        self.assertIn("COUNT(DISTINCT IF(network_type = 'waterfall', TO_JSON_STRING(STRUCT(network_type, network)), NULL)) AS waterfall_cnt", metric2_sql)
        self.assertNotIn("COUNT(DISTINCT STRUCT(", metric2_sql)
        self.assertNotIn("qualified_requests", metric2_sql)
        self.assertIn("COUNT(DISTINCT TO_JSON_STRING(STRUCT(r.network_type, r.network))) AS type_network_cnt", metric3_sql)
        self.assertNotIn("COUNT(DISTINCT STRUCT(", metric3_sql)
        self.assertNotIn("qualified_requests", metric3_sql)
        self.assertIn("COUNT(*) AS placement_cnt", metric1_placement_sql)
        self.assertNotIn("COUNT(DISTINCT placement", metric1_placement_sql)
        self.assertIn("COUNT(DISTINCT request_id) AS denominator_request_pv", metric1_placement_sql)
        self.assertIn("COUNT(DISTINCT request_id) AS request_pv", metric1_placement_sql)
        self.assertIn("COUNT(*) AS placement_cnt", metric2_placement_sql)
        self.assertIn("COUNTIF(network_type = 'bidding') AS bidding_placement_cnt", metric2_placement_sql)
        self.assertIn("COUNTIF(network_type = 'waterfall') AS waterfall_placement_cnt", metric2_placement_sql)
        self.assertIn("COUNT(DISTINCT request_id) AS denominator_request_pv", metric2_placement_sql)
        self.assertIn("COUNT(DISTINCT request_id) AS request_pv", metric2_placement_sql)
        self.assertIn("COUNT(*) AS type_placement_cnt", metric3_placement_sql)
        self.assertIn("COUNT(DISTINCT request_id) AS denominator_request_pv", metric3_placement_sql)
        self.assertIn("COUNT(DISTINCT request_id) AS request_pv", metric3_placement_sql)
        self.assertNotIn("waterfall_status_priority", metric3_placement_sql)
        self.assertNotIn("status_rank", metric3_placement_sql)

    def test_request_structure_sql_adds_metric1_success_rank_distribution(self) -> None:
        """新增 SQL 应按 cnt_type + cnt_value 产出 success_rank 分布与桶级成功率分母。"""
        sql = (PROJECT_DIR / "sql" / "metric1_success_rank_distribution.sql").read_text(encoding="utf-8")

        self.assertIn("status IN ('AD_LOADED', 'FAILED_TO_LOAD', 'AD_LOAD_NOT_ATTEMPTED')", sql)
        self.assertIn("COUNT(DISTINCT CONCAT(network_type, '||', network)) AS network_cnt", sql)
        self.assertIn("COUNT(*) AS placement_cnt", sql)
        self.assertIn("status = 'FAILED_TO_LOAD'", sql)
        self.assertIn("status = 'AD_LOADED'", sql)
        self.assertIn("success_rank", sql)
        self.assertIn("'network' AS cnt_type", sql)
        self.assertIn("'placement' AS cnt_type", sql)
        self.assertIn("bucket_success_request_pv", sql)
        self.assertIn("bucket_total_request_pv", sql)
        self.assertNotIn("success_target", sql)

    def test_request_structure_page_script_aligns_metric1_with_country_layout(self) -> None:
        """请求结构页应保留前两张旧图，并新增独立的 success_rank 双图区块。"""
        script = dashboard.build_request_structure_page_script()
        self.assertIn("function mountDistributionCompareChart", script)
        self.assertIn("function resolveDistributionAxisMax", script)
        self.assertIn("network个数", script)
        self.assertIn("placement个数", script)
        self.assertNotIn("成功+失败placement个数", script)
        self.assertNotIn("placement_success_failed_view", script)
        self.assertIn("renderMetric1RankBlock", script)
        self.assertIn("network_cnt（单选）", script)
        self.assertIn("placement_cnt（单选）", script)
        self.assertIn("success_rank", script)
        self.assertIn("network_type（单选）", script)
        self.assertIn("type_network_cnt（单选）", script)
        self.assertIn("placement_cnt（单选）", script)
        self.assertIn("type_placement_cnt（单选）", script)
        self.assertNotIn("A / B 同图", script)
        self.assertIn("calcMetric1ChartWidth", script)
        self.assertIn("chart-scroll", script)
        self.assertIn("renderMetric1View(stack,'network个数'", script)
        self.assertIn("renderMetric1View(stack,'placement个数'", script)
        self.assertIn("renderMetric1RankView", script)
        self.assertNotIn("status（多选）", script)
        self.assertNotIn("当前 status 组合", script)
        self.assertNotIn("左图固定 A 组", script)
        self.assertNotIn("当前 ${view.count_label}", script)

    def test_request_structure_page_script_adds_success_scope_selector_for_metric1234(self) -> None:
        """请求结构页应新增 success scope selector，并影响 metric1/2/3/4。"""
        script = dashboard.build_request_structure_page_script()
        self.assertIn("success-scope-select", script)
        self.assertIn("combo('metric1',product,adFormat,'',successScope)", script)
        self.assertIn("combo('metric2',product,adFormat,'',successScope)", script)
        self.assertIn("combo('metric3',product,adFormat,'',successScope)", script)
        self.assertIn("c4=combo('metric4',product,adFormat,'',successScope)", script)
        self.assertIn("c5=combo('metric5',product,adFormat)", script)
        self.assertIn("c6=combo('metric6',product,adFormat)", script)
        self.assertIn("successScopeLabel", script)

    def test_normalize_metric1_bucket_key_keeps_real_placement_count(self) -> None:
        """请求结构页 placement 横轴应固定保留 1-35，并把更大的值归到 35+。"""
        self.assertEqual(dashboard.normalize_metric1_bucket_key("placement_cnt", "35"), "35")
        self.assertEqual(dashboard.normalize_metric1_bucket_key("placement_cnt", "36"), "35+")
        self.assertEqual(dashboard.normalize_metric1_bucket_key("placement_cnt_bucket", "40"), "40")

    def test_request_structure_payload_uses_user_friendly_axis_labels(self) -> None:
        """请求结构 payload 的 tooltip 轴标签应展示业务口径，而不是原始字段名。"""
        metric1 = dashboard.build_request_structure_metric1(
            [
                {
                    "experiment_group": dashboard.GROUP_A,
                    "product": "demo",
                    "ad_format": "interstitial",
                    "country": "US",
                    "network_cnt": "3",
                    "request_pv": "5",
                    "denominator_request_pv": "10",
                    "share": "0.5",
                }
            ],
            [
                {
                    "experiment_group": dashboard.GROUP_A,
                    "product": "demo",
                    "ad_format": "interstitial",
                    "country": "US",
                    "placement_cnt": "7",
                    "request_pv": "5",
                    "denominator_request_pv": "10",
                    "share": "0.5",
                }
            ],
            [
                {
                    "experiment_group": dashboard.GROUP_A,
                    "product": "demo",
                    "ad_format": "interstitial",
                    "country": "US",
                    "placement_cnt_bucket": "22+",
                    "request_pv": "3",
                    "denominator_request_pv": "10",
                    "share": "0.3",
                }
            ],
        )
        combo1 = metric1["combos"]["demo__interstitial__US"]
        self.assertEqual(
            combo1["network_view"]["groups"][dashboard.GROUP_A]["points"][0]["axis_label"],
            "network个数",
        )
        self.assertEqual(
            combo1["placement_view"]["groups"][dashboard.GROUP_A]["points"][0]["axis_label"],
            "placement个数",
        )
        self.assertEqual(
            combo1["rank_block"]["network_rank_view"]["bucket_map"]["3"]["groups"][dashboard.GROUP_A]["points"][0]["axis_label"],
            "success_rank",
        )

        metric2 = dashboard.build_request_structure_metric2(
            [
                {
                    "experiment_group": dashboard.GROUP_A,
                    "product": "demo",
                    "ad_format": "interstitial",
                    "country": "US",
                    "network_cnt": "3",
                    "bidding_cnt": "1",
                    "waterfall_cnt": "2",
                    "request_pv": "5",
                    "denominator_request_pv": "10",
                    "share": "0.5",
                }
            ],
            [
                {
                    "experiment_group": dashboard.GROUP_A,
                    "product": "demo",
                    "ad_format": "interstitial",
                    "country": "US",
                    "placement_cnt": "3",
                    "bidding_placement_cnt": "1",
                    "waterfall_placement_cnt": "2",
                    "request_pv": "5",
                    "denominator_request_pv": "10",
                    "share": "0.5",
                }
            ],
        )
        combo2 = metric2["combos"]["demo__interstitial__US"]
        self.assertEqual(combo2["network_view"]["bucket_label"], "Bx+Wy")
        self.assertEqual(combo2["placement_view"]["bucket_label"], "Bx+Wy")
        self.assertEqual(
            combo2["network_view"]["cnt_map"]["3"]["groups"][dashboard.GROUP_A]["points"][0]["axis_label"],
            "Bx+Wy",
        )
        self.assertEqual(
            combo2["placement_view"]["cnt_map"]["3"]["groups"][dashboard.GROUP_A]["points"][0]["axis_label"],
            "Bx+Wy",
        )

    def test_request_structure_page_script_removes_22_plus_copy(self) -> None:
        """请求结构页普通版文案应改成固定展示 1-35，并将更大值合并到 35+。"""
        script = dashboard.build_request_structure_page_script()
        self.assertNotIn("固定展示 1-35，35 以上合并为 35+", script)
        self.assertNotIn("31+ / 41+", script)

    def test_request_structure_page_script_uses_scroll_instead_of_initial_bar_zoom(self) -> None:
        """请求结构柱图应直接显示完整横轴，不再做初始 dataZoom 截断。"""
        script = dashboard.build_request_structure_page_script()
        self.assertNotIn("dataZoom:[{type:'inside'},{type:'slider'", script)
        self.assertNotIn("A / B 同图", script)
        self.assertNotIn("左右 selector 独立", script)
        self.assertNotIn("B{bidding_placement_cnt}+W{waterfall_placement_cnt}", script)

    def test_request_structure_html_removes_req_index_and_old_200_limit_copy(self) -> None:
        """请求结构分布页不应再出现 req_index、1-200 和请求轮次文案。"""
        html = dashboard.build_dashboard_html(
            page_title="请求结构分布",
            metrics={
                "metric1": {"title": "新1", "desc": dashboard.REQUEST_STRUCTURE_TEXT["metric1"], "combos": {}, "network_cnt_options": []},
                "metric2": {"title": "新2", "desc": dashboard.REQUEST_STRUCTURE_TEXT["metric2"], "combos": {}, "network_cnt_options": []},
                "metric3": {"title": "新3", "desc": dashboard.REQUEST_STRUCTURE_TEXT["metric3"], "combos": {}, "network_cnt_options": []},
                "metric4": {"title": "旧4", "desc": dashboard.REQUEST_STRUCTURE_TEXT["metric4"], "combos": {}, "table_mode": "pivot"},
                "metric5": {"title": "旧5", "desc": dashboard.REQUEST_STRUCTURE_TEXT["metric5"], "combos": {}, "table_mode": "matrix"},
            },
            products=["demo"],
            ad_formats=["interstitial"],
            page_key="request_structure",
        )
        self.assertNotIn("req_index", html)
        self.assertNotIn("1-200", html)
        self.assertNotIn("请求轮次", html)

    def test_request_structure_html_contains_success_scope_selector(self) -> None:
        """请求结构分布页应渲染 success scope selector。"""
        html = dashboard.build_dashboard_html(
            page_title="请求结构分布",
            metrics={
                "metric1": {"title": "新1", "desc": dashboard.REQUEST_STRUCTURE_TEXT["metric1"], "combos": {}},
                "metric2": {"title": "新2", "desc": dashboard.REQUEST_STRUCTURE_TEXT["metric2"], "combos": {}},
                "metric3": {"title": "新3", "desc": dashboard.REQUEST_STRUCTURE_TEXT["metric3"], "combos": {}},
                "metric4": {"title": "旧4", "desc": dashboard.REQUEST_STRUCTURE_TEXT["metric4"], "combos": {}, "table_mode": "pivot"},
                "metric5": {"title": "旧5", "desc": dashboard.REQUEST_STRUCTURE_TEXT["metric5"], "combos": {}, "table_mode": "matrix"},
            },
            products=["demo"],
            ad_formats=["interstitial"],
            success_scopes=["all", "has_success", "no_success"],
            page_key="request_structure",
        )
        self.assertIn('for="success-scope-select"', html)
        self.assertIn('id="success-scope-select"', html)

    def test_request_structure_metric1_uses_success_scope_combo_keys(self) -> None:
        """请求结构页 metric1 应按 success_scope 拆分 combo key。"""
        payload = dashboard.build_request_structure_metric1(
            [
                {
                    "experiment_group": dashboard.GROUP_A,
                    "product": "demo",
                    "ad_format": "interstitial",
                    "success_scope": "has_success",
                    "network_cnt": "3",
                    "request_pv": "4",
                    "denominator_request_pv": "10",
                    "share": "0.4",
                },
                {
                    "experiment_group": dashboard.GROUP_A,
                    "product": "demo",
                    "ad_format": "interstitial",
                    "success_scope": "no_success",
                    "network_cnt": "4",
                    "request_pv": "6",
                    "denominator_request_pv": "12",
                    "share": "0.5",
                },
            ],
            [],
            [],
        )
        self.assertIn("demo__interstitial__has_success", payload["combos"])
        self.assertIn("demo__interstitial__no_success", payload["combos"])
        self.assertEqual(payload["combos"]["demo__interstitial__has_success"]["success_scope"], "has_success")
        self.assertEqual(payload["combos"]["demo__interstitial__no_success"]["success_scope"], "no_success")

    def test_request_structure_sql_files_add_success_scope_dimension(self) -> None:
        """请求结构页 metric1/2/3 SQL 应增加 success_scope 维度。"""
        file_names = [
            "metric1_total_network_cnt_distribution.sql",
            "metric1_total_placement_cnt_distribution.sql",
            "metric1_success_rank_distribution.sql",
            "metric2_type_mix_distribution.sql",
            "metric2_type_placement_mix_distribution.sql",
            "metric3_type_status_distribution.sql",
            "metric3_type_placement_status_distribution.sql",
        ]
        for file_name in file_names:
            sql = (PROJECT_DIR / "sql" / file_name).read_text(encoding="utf-8")
            self.assertIn("success_scope", sql, file_name)
            self.assertIn("AD_LOADED", sql, file_name)

    def test_request_structure_page_renders_metric4_metric5_metric6_in_order(self) -> None:
        """请求结构页应按新顺序展示 metric4、metric5、metric6。"""
        script = dashboard.build_request_structure_page_script()
        self.assertLess(script.index("if(c4)renderMetric4(root,c4)"), script.index("if(c5)renderMetric5(root,c5)"))
        self.assertLess(script.index("if(c5)renderMetric5(root,c5)"), script.index("if(c6)renderMetric6(root,c6)"))

    def test_request_structure_country_page_script_uses_country_selector_and_renders_metric4(self) -> None:
        """country 版请求结构页应增加 country selector，并渲染到 metric4。"""
        script = dashboard.build_request_structure_country_page_script()
        self.assertIn("country-select", script)
        self.assertIn("combo('metric1',product,adFormat,country)", script)
        self.assertIn("combo('metric2',product,adFormat,country)", script)
        self.assertIn("combo('metric3',product,adFormat,country)", script)
        self.assertIn("combo('metric4',product,adFormat,country)", script)
        self.assertIn("network个数", script)
        self.assertIn("placement个数", script)
        self.assertNotIn("成功+失败placement个数", script)
        self.assertIn("renderMetric1RankBlock", script)
        self.assertIn("calcMetric1ChartWidth", script)
        self.assertIn("chart-scroll", script)
        self.assertIn("appendScrollableDistributionChart(chartWrap,box,current)", script)
        self.assertIn("if(c4)renderMetric4(root,c4)", script)
        self.assertNotIn("if(c5)renderMetric5(root,c5)", script)
        self.assertIn("renderMetric1View(stack,'network个数'", script)
        self.assertIn("renderMetric1View(stack,'placement个数'", script)
        self.assertIn("renderMetric1RankView", script)

    def test_request_structure_country_metric1_merges_bucket_categories_for_x_axis(self) -> None:
        """country 版 metric1 横轴应固定生成 1-35 和 35+ 文案。"""
        script = dashboard.build_request_structure_country_page_script()
        self.assertIn("const categorySet=new Set()", script)
        self.assertIn("const pointMap=new Map()", script)
        self.assertIn("return categories.map(bucketKey=>{", script)
        self.assertNotIn("固定展示 1-35，35 以上合并为 35+", script)

    def test_request_structure_country_metric1_uses_aggressive_scroll_width(self) -> None:
        """country 版 metric1 不应再按桶数放大图宽。"""
        script = dashboard.build_request_structure_country_page_script()
        self.assertIn("function calcMetric1ChartWidth(view){void view;return 0;}", script)

    def test_build_request_structure_country_payload_limits_country_options_to_top10(self) -> None:
        """country 版 payload 每个 product + ad_format 只保留 request_pv 总量 TOP10 国家。"""
        metric1_rows = []
        for index in range(12):
            country = f"C{index:02d}"
            metric1_rows.append(
                {
                    "experiment_group": dashboard.GROUP_A,
                    "product": "demo",
                    "ad_format": "interstitial",
                    "country": country,
                    "network_cnt": "4",
                    "request_pv": str(120 - index),
                    "denominator_request_pv": "500",
                    "share": "0.2",
                }
            )
            metric1_rows.append(
                {
                    "experiment_group": dashboard.GROUP_B,
                    "product": "demo",
                    "ad_format": "interstitial",
                    "country": country,
                    "network_cnt": "4",
                    "request_pv": str(60 - index),
                    "denominator_request_pv": "400",
                    "share": "0.15",
                }
            )
        with mock.patch.object(
            dashboard,
            "load_optional_rows",
            side_effect=[metric1_rows, [], [], [], [], [], [], [], []],
        ):
            payload = dashboard.build_request_structure_country_payload()

        expected_countries = [f"C{index:02d}" for index in range(10)]
        self.assertEqual(
            payload["country_options_by_combo"]["demo__interstitial"],
            expected_countries,
        )
        self.assertEqual(payload["countries"], expected_countries)
        self.assertIn("demo__interstitial__C00", payload["metrics"]["metric1"]["combos"])
        self.assertNotIn("demo__interstitial__C10", payload["metrics"]["metric1"]["combos"])

    def test_build_request_structure_country_payload_uses_fixed_35_plus_tail(self) -> None:
        """country 版 payload 的 metric1 placement 应固定展示 1-35，并把更大的值合并到 35+。"""
        network_rows = [
            {
                "experiment_group": dashboard.GROUP_A,
                "product": "demo",
                "ad_format": "interstitial",
                "country": "United States",
                "network_cnt": "4",
                "request_pv": "100",
                "denominator_request_pv": "100",
                "share": "1",
            }
        ]
        placement_rows = [
                {
                    "experiment_group": dashboard.GROUP_A,
                    "product": "demo",
                    "ad_format": "interstitial",
                    "country": "United States",
                    "placement_cnt": "6",
                    "request_pv": "95",
                    "denominator_request_pv": "100",
                    "share": "0.95",
            },
                {
                    "experiment_group": dashboard.GROUP_A,
                    "product": "demo",
                    "ad_format": "interstitial",
                    "country": "United States",
                    "placement_cnt": "35",
                    "request_pv": "1",
                    "denominator_request_pv": "100",
                    "share": "0.01",
            },
                {
                    "experiment_group": dashboard.GROUP_A,
                    "product": "demo",
                    "ad_format": "interstitial",
                    "country": "United States",
                    "placement_cnt": "36",
                    "request_pv": "1",
                    "denominator_request_pv": "100",
                    "share": "0.01",
            },
        ]
        with mock.patch.object(
            dashboard,
            "load_optional_rows",
            side_effect=[network_rows, placement_rows, [], [], [], [], [], [], []],
        ):
            payload = dashboard.build_request_structure_country_payload()

        placement_view = payload["metrics"]["metric1"]["combos"]["demo__interstitial__United States"]["placement_view"]
        self.assertEqual(placement_view["count_options"][:6], ["1", "2", "3", "4", "5", "6"])
        self.assertEqual(placement_view["count_options"][-2:], ["35", "35+"])

    def test_request_structure_country_html_contains_country_selector(self) -> None:
        """country 版请求结构 HTML 应渲染独立 country selector。"""
        html = dashboard.build_dashboard_html(
            page_title="请求结构分布（Country）",
            metrics={
                "metric1": {"title": "新1", "desc": dashboard.REQUEST_STRUCTURE_TEXT["metric1"], "combos": {}},
                "metric2": {"title": "新2", "desc": dashboard.REQUEST_STRUCTURE_TEXT["metric2"], "combos": {}},
                "metric3": {"title": "新3", "desc": dashboard.REQUEST_STRUCTURE_TEXT["metric3"], "combos": {}},
            },
            products=["demo"],
            ad_formats=["interstitial"],
            countries=["US"],
            country_options_by_combo={"demo__interstitial": ["US"]},
            page_key="request_structure_country",
        )
        self.assertIn("AB Request Structure Country Dashboard", html)
        self.assertIn('for="country-select"', html)
        self.assertIn('id="country-select"', html)

    def test_request_structure_unit_page_script_uses_unit_selector_and_renders_metric4(self) -> None:
        """unit 版请求结构页应增加 unit selector，并渲染到 metric4。"""
        script = dashboard.build_request_structure_unit_page_script()
        self.assertIn("unit-select", script)
        self.assertIn("unit=document.getElementById('unit-select').value", script)
        self.assertNotIn("country=document.getElementById('unit-select').value", script)
        self.assertIn("combo('metric1',product,adFormat,unit)", script)
        self.assertIn("combo('metric2',product,adFormat,unit)", script)
        self.assertIn("combo('metric3',product,adFormat,unit)", script)
        self.assertIn("combo('metric4',product,adFormat,unit)", script)
        self.assertIn("network个数", script)
        self.assertIn("placement个数", script)
        self.assertNotIn("成功+失败placement个数", script)
        self.assertIn("renderMetric1RankBlock", script)
        self.assertIn("calcMetric1ChartWidth", script)
        self.assertIn("chart-scroll", script)
        self.assertIn("appendScrollableDistributionChart(chartWrap,box,current)", script)
        self.assertIn("if(c4)renderMetric4(root,c4)", script)
        self.assertNotIn("if(c5)renderMetric5(root,c5)", script)

    def test_build_request_structure_unit_payload_uses_fixed_35_plus_tail(self) -> None:
        """unit 版 payload 的 metric1 placement 应固定展示 1-35，并把更大的值合并到 35+。"""
        network_rows = [
            {
                "experiment_group": dashboard.GROUP_A,
                "product": "demo",
                "ad_format": "interstitial",
                "max_unit_id": "u1",
                "network_cnt": "4",
                "request_pv": "100",
                "denominator_request_pv": "100",
                "share": "1",
            }
        ]
        placement_rows = [
                {
                    "experiment_group": dashboard.GROUP_A,
                    "product": "demo",
                    "ad_format": "interstitial",
                    "max_unit_id": "u1",
                    "placement_cnt": "6",
                    "request_pv": "95",
                    "denominator_request_pv": "100",
                    "share": "0.95",
            },
                {
                    "experiment_group": dashboard.GROUP_A,
                    "product": "demo",
                    "ad_format": "interstitial",
                    "max_unit_id": "u1",
                    "placement_cnt": "35",
                    "request_pv": "1",
                    "denominator_request_pv": "100",
                    "share": "0.01",
            },
                {
                    "experiment_group": dashboard.GROUP_A,
                    "product": "demo",
                    "ad_format": "interstitial",
                    "max_unit_id": "u1",
                    "placement_cnt": "36",
                    "request_pv": "1",
                    "denominator_request_pv": "100",
                    "share": "0.01",
            },
        ]
        with mock.patch.object(dashboard, "load_ad_unit_name_map", return_value={"u1": "Demo Unit P1"}):
            with mock.patch.object(
                dashboard,
                "load_optional_rows",
                side_effect=[network_rows, placement_rows, [], [], [], [], [], [], []],
            ):
                payload = dashboard.build_request_structure_unit_payload()

        placement_view = payload["metrics"]["metric1"]["combos"]["demo__interstitial__Demo Unit P1"]["placement_view"]
        self.assertEqual(placement_view["count_options"][:6], ["1", "2", "3", "4", "5", "6"])
        self.assertEqual(placement_view["count_options"][-2:], ["35", "35+"])

    def test_request_structure_unit_html_contains_unit_selector(self) -> None:
        """unit 版请求结构 HTML 应渲染独立 unit selector。"""
        html = dashboard.build_dashboard_html(
            page_title="请求结构分布（Unit）",
            metrics={
                "metric1": {"title": "新1", "desc": dashboard.REQUEST_STRUCTURE_TEXT["metric1"], "combos": {}},
                "metric2": {"title": "新2", "desc": dashboard.REQUEST_STRUCTURE_TEXT["metric2"], "combos": {}},
                "metric3": {"title": "新3", "desc": dashboard.REQUEST_STRUCTURE_TEXT["metric3"], "combos": {}},
            },
            products=["demo"],
            ad_formats=["interstitial"],
            page_key="request_structure_unit",
            units=["Demo Unit"],
            unit_options_by_combo={"demo__interstitial": ["Demo Unit"]},
        )
        self.assertIn("AB Request Structure Unit Dashboard", html)
        self.assertIn('for="unit-select"', html)
        self.assertIn('id="unit-select"', html)

    def test_build_request_structure_unit_payload_maps_unit_name_in_python(self) -> None:
        """unit payload 应在 Python 内把 max_unit_id 替换成可读 unit 名。"""
        rows = [
            {
                "experiment_group": dashboard.GROUP_A,
                "product": "demo",
                "ad_format": "interstitial",
                "max_unit_id": "u1",
                "network_cnt": "4",
                "request_pv": "7",
                "denominator_request_pv": "10",
                "share": "0.7",
            }
        ]
        with mock.patch.object(dashboard, "load_optional_rows", side_effect=[rows, [], [], [], [], [], [], [], []]):
            with mock.patch.object(dashboard, "load_ad_unit_name_map", return_value={"u1": "Demo Unit P1"}):
                payload = dashboard.build_request_structure_unit_payload()

        self.assertEqual(payload["units"], ["Demo Unit P1"])
        self.assertEqual(payload["unit_options_by_combo"], {"demo__interstitial": ["Demo Unit P1"]})
        combo = payload["metrics"]["metric1"]["combos"]["demo__interstitial__Demo Unit P1"]
        self.assertEqual(combo["country"], "Demo Unit P1")
        self.assertEqual(combo["max_unit_id"], "u1")

    def test_build_request_structure_unit_payload_falls_back_to_unit_id_when_mapping_missing(self) -> None:
        """unit 名缺失映射时应回退显示 max_unit_id。"""
        rows = [
            {
                "experiment_group": dashboard.GROUP_A,
                "product": "demo",
                "ad_format": "interstitial",
                "max_unit_id": "u_missing",
                "network_cnt": "4",
                "request_pv": "7",
                "denominator_request_pv": "10",
                "share": "0.7",
            }
        ]
        with mock.patch.object(dashboard, "load_optional_rows", side_effect=[rows, [], [], [], [], [], [], [], []]):
            with mock.patch.object(dashboard, "load_ad_unit_name_map", return_value={}):
                payload = dashboard.build_request_structure_unit_payload()

        self.assertEqual(payload["units"], ["u_missing"])
        combo = payload["metrics"]["metric1"]["combos"]["demo__interstitial__u_missing"]
        self.assertEqual(combo["country"], "u_missing")
        self.assertEqual(combo["max_unit_id"], "u_missing")

    def test_build_request_structure_unit_payload_skips_empty_unit_id(self) -> None:
        """空 max_unit_id 不应进入 unit selector 或 combo。"""
        rows = [
            {
                "experiment_group": dashboard.GROUP_A,
                "product": "demo",
                "ad_format": "interstitial",
                "max_unit_id": "",
                "network_cnt": "4",
                "request_pv": "7",
                "denominator_request_pv": "10",
                "share": "0.7",
            }
        ]
        with mock.patch.object(dashboard, "load_optional_rows", side_effect=[rows, [], [], [], [], [], [], [], []]):
            with mock.patch.object(dashboard, "load_ad_unit_name_map", return_value={}):
                payload = dashboard.build_request_structure_unit_payload()

        self.assertEqual(payload["units"], [])
        self.assertEqual(payload["metrics"]["metric1"]["combos"], {})

    def test_request_structure_country_metric1_keeps_rank_cnt_source_buckets(self) -> None:
        """country 维度下 placement 总览固定到 35+，rank 下拉仍保留源 cnt 值。"""
        payload = dashboard.build_request_structure_metric1(
            [],
            [
                {
                    "experiment_group": dashboard.GROUP_A,
                    "product": "demo",
                    "ad_format": "interstitial",
                    "country": "US",
                    "placement_cnt": "6",
                    "request_pv": "3",
                    "denominator_request_pv": "10",
                    "share": "0.3",
                },
                {
                    "experiment_group": dashboard.GROUP_A,
                    "product": "demo",
                    "ad_format": "interstitial",
                    "country": "US",
                    "placement_cnt": "35",
                    "request_pv": "4",
                    "denominator_request_pv": "10",
                    "share": "0.4",
                },
                {
                    "experiment_group": dashboard.GROUP_A,
                    "product": "demo",
                    "ad_format": "interstitial",
                    "country": "US",
                    "placement_cnt": "36",
                    "request_pv": "1",
                    "denominator_request_pv": "10",
                    "share": "0.1",
                },
            ],
            [
                {
                    "experiment_group": dashboard.GROUP_A,
                    "product": "demo",
                    "ad_format": "interstitial",
                    "country": "US",
                    "cnt_type": "placement",
                    "cnt_value": "35",
                    "success_rank": "2",
                    "request_pv": "2",
                    "bucket_success_request_pv": "2",
                    "bucket_total_request_pv": "4",
                },
                {
                    "experiment_group": dashboard.GROUP_A,
                    "product": "demo",
                    "ad_format": "interstitial",
                    "country": "US",
                    "cnt_type": "placement",
                    "cnt_value": "36",
                    "success_rank": "3",
                    "request_pv": "1",
                    "bucket_success_request_pv": "1",
                    "bucket_total_request_pv": "1",
                },
            ],
        )

        combo = payload["combos"]["demo__interstitial__US"]
        placement_view = combo["placement_view"]
        placement_rank_view = combo["rank_block"]["placement_rank_view"]
        self.assertEqual(placement_view["count_options"][:6], ["1", "2", "3", "4", "5", "6"])
        self.assertEqual(placement_view["count_options"][-2:], ["35", "35+"])
        self.assertEqual(placement_rank_view["cnt_options"], ["35", "36"])

    def test_run_ab_dashboard_sql_uses_new_request_structure_sql_files(self) -> None:
        """SQL runner 默认应执行 network 与 placement 两套请求结构 SQL。"""
        self.assertEqual(
            runner.SQL_TO_CSV["metric1_total_network_cnt_distribution.sql"],
            "metric1_request_network_cnt.csv",
        )
        self.assertEqual(
            runner.SQL_TO_CSV["metric2_type_mix_distribution.sql"],
            "metric2_network_type_status_cnt.csv",
        )
        self.assertEqual(
            runner.SQL_TO_CSV["metric3_type_status_distribution.sql"],
            "metric3_network_distribution.csv",
        )
        self.assertEqual(
            runner.SQL_TO_CSV["metric1_total_placement_cnt_distribution.sql"],
            "metric1_request_placement_cnt.csv",
        )
        self.assertEqual(
            runner.SQL_TO_CSV["metric1_success_rank_distribution.sql"],
            "metric1_success_rank_distribution.csv",
        )
        self.assertEqual(
            runner.SQL_TO_CSV["metric2_type_placement_mix_distribution.sql"],
            "metric2_type_placement_status_cnt.csv",
        )
        self.assertEqual(
            runner.SQL_TO_CSV["metric3_type_placement_status_distribution.sql"],
            "metric3_placement_distribution.csv",
        )
        self.assertNotIn("metric1_network_cnt_share.sql", runner.SQL_TO_CSV)
        self.assertNotIn("metric2_network_type_share.sql", runner.SQL_TO_CSV)
        self.assertNotIn("metric3_network_distribution.sql", runner.SQL_TO_CSV)

    def test_run_ab_dashboard_sql_registers_country_request_structure_sql_files(self) -> None:
        """SQL runner 应注册 country 版请求结构 SQL 与 CSV。"""
        self.assertEqual(
            runner.SQL_TO_CSV["metric1_total_network_cnt_distribution_country.sql"],
            "metric1_request_network_cnt_country.csv",
        )
        self.assertEqual(
            runner.SQL_TO_CSV["metric1_total_placement_cnt_distribution_country.sql"],
            "metric1_request_placement_cnt_country.csv",
        )
        self.assertEqual(
            runner.SQL_TO_CSV["metric1_success_rank_distribution_country.sql"],
            "metric1_success_rank_distribution_country.csv",
        )
        self.assertEqual(
            runner.SQL_TO_CSV["metric2_type_mix_distribution_country.sql"],
            "metric2_network_type_status_cnt_country.csv",
        )
        self.assertEqual(
            runner.SQL_TO_CSV["metric2_type_placement_mix_distribution_country.sql"],
            "metric2_type_placement_status_cnt_country.csv",
        )
        self.assertEqual(
            runner.SQL_TO_CSV["metric3_type_status_distribution_country.sql"],
            "metric3_network_distribution_country.csv",
        )
        self.assertEqual(
            runner.SQL_TO_CSV["metric3_type_placement_status_distribution_country.sql"],
            "metric3_placement_distribution_country.csv",
        )

    def test_run_ab_dashboard_sql_registers_unit_request_structure_sql_files(self) -> None:
        """SQL runner 应注册 unit 版请求结构 SQL 与 CSV。"""
        self.assertEqual(
            runner.SQL_TO_CSV["metric1_total_network_cnt_distribution_unit.sql"],
            "metric1_request_network_cnt_unit.csv",
        )
        self.assertEqual(
            runner.SQL_TO_CSV["metric1_total_placement_cnt_distribution_unit.sql"],
            "metric1_request_placement_cnt_unit.csv",
        )
        self.assertEqual(
            runner.SQL_TO_CSV["metric1_success_rank_distribution_unit.sql"],
            "metric1_success_rank_distribution_unit.csv",
        )
        self.assertEqual(
            runner.SQL_TO_CSV["metric2_type_mix_distribution_unit.sql"],
            "metric2_network_type_status_cnt_unit.csv",
        )
        self.assertEqual(
            runner.SQL_TO_CSV["metric2_type_placement_mix_distribution_unit.sql"],
            "metric2_type_placement_status_cnt_unit.csv",
        )
        self.assertEqual(
            runner.SQL_TO_CSV["metric3_type_status_distribution_unit.sql"],
            "metric3_network_distribution_unit.csv",
        )
        self.assertEqual(
            runner.SQL_TO_CSV["metric3_type_placement_status_distribution_unit.sql"],
            "metric3_placement_distribution_unit.csv",
        )

    def test_run_ab_dashboard_sql_registers_metric4_drilldown_sql_files(self) -> None:
        """SQL runner 应注册新 metric4 的 network/placement 下钻 SQL。"""
        self.assertEqual(
            runner.SQL_TO_CSV["metric4_cnt_level_network_distribution.sql"],
            "metric4_cnt_level_network_distribution.csv",
        )
        self.assertEqual(
            runner.SQL_TO_CSV["metric4_cnt_level_placement_distribution.sql"],
            "metric4_cnt_level_placement_distribution.csv",
        )
        self.assertEqual(
            runner.SQL_TO_CSV["metric4_cnt_level_network_distribution_country.sql"],
            "metric4_cnt_level_network_distribution_country.csv",
        )
        self.assertEqual(
            runner.SQL_TO_CSV["metric4_cnt_level_placement_distribution_country.sql"],
            "metric4_cnt_level_placement_distribution_country.csv",
        )
        self.assertEqual(
            runner.SQL_TO_CSV["metric4_cnt_level_network_distribution_unit.sql"],
            "metric4_cnt_level_network_distribution_unit.csv",
        )
        self.assertEqual(
            runner.SQL_TO_CSV["metric4_cnt_level_placement_distribution_unit.sql"],
            "metric4_cnt_level_placement_distribution_unit.csv",
        )

    def test_request_structure_country_sql_files_add_country_dimension(self) -> None:
        """country 版请求结构 SQL 应统一新增 country 维度并把 NULL 归到 UNKNOWN。"""
        metric1_sql = (PROJECT_DIR / "sql" / "metric1_total_network_cnt_distribution_country.sql").read_text(encoding="utf-8")
        metric1_placement_sql = (PROJECT_DIR / "sql" / "metric1_total_placement_cnt_distribution_country.sql").read_text(encoding="utf-8")
        metric1_rank_sql = (PROJECT_DIR / "sql" / "metric1_success_rank_distribution_country.sql").read_text(encoding="utf-8")
        metric2_sql = (PROJECT_DIR / "sql" / "metric2_type_mix_distribution_country.sql").read_text(encoding="utf-8")
        metric2_placement_sql = (PROJECT_DIR / "sql" / "metric2_type_placement_mix_distribution_country.sql").read_text(encoding="utf-8")
        metric3_sql = (PROJECT_DIR / "sql" / "metric3_type_status_distribution_country.sql").read_text(encoding="utf-8")
        metric3_placement_sql = (PROJECT_DIR / "sql" / "metric3_type_placement_status_distribution_country.sql").read_text(encoding="utf-8")

        self.assertIn("COALESCE(country, 'UNKNOWN') AS country", metric1_sql)
        self.assertIn("GROUP BY experiment_group, product, ad_format, country", metric1_sql)
        self.assertIn("COALESCE(country, 'UNKNOWN') AS country", metric1_placement_sql)
        self.assertIn("COALESCE(country, 'UNKNOWN') AS country", metric1_rank_sql)
        self.assertIn("PARTITION BY", metric1_rank_sql)
        self.assertIn("p.country", metric1_rank_sql)
        self.assertIn("cnt_type", metric1_rank_sql)
        self.assertIn("COALESCE(country, 'UNKNOWN') AS country", metric2_sql)
        self.assertIn("GROUP BY experiment_group, product, ad_format, country, network_cnt", metric2_sql)
        self.assertIn("COALESCE(country, 'UNKNOWN') AS country", metric2_placement_sql)
        self.assertIn("GROUP BY experiment_group, product, ad_format, country, placement_cnt", metric2_placement_sql)
        self.assertIn("COALESCE(country, 'UNKNOWN') AS country", metric3_sql)
        self.assertIn("GROUP BY experiment_group, product, ad_format, country, network_cnt, network_type, type_network_cnt", metric3_sql)
        self.assertIn("COALESCE(country, 'UNKNOWN') AS country", metric3_placement_sql)
        self.assertIn("GROUP BY experiment_group, product, ad_format, country, placement_cnt, network_type, type_placement_cnt", metric3_placement_sql)

    def test_request_structure_unit_sql_files_add_unit_dimension(self) -> None:
        """unit 版请求结构 SQL 应统一使用 max_unit_id 维度并过滤空值。"""
        metric1_sql = (PROJECT_DIR / "sql" / "metric1_total_network_cnt_distribution_unit.sql").read_text(encoding="utf-8")
        metric1_placement_sql = (PROJECT_DIR / "sql" / "metric1_total_placement_cnt_distribution_unit.sql").read_text(encoding="utf-8")
        metric1_rank_sql = (PROJECT_DIR / "sql" / "metric1_success_rank_distribution_unit.sql").read_text(encoding="utf-8")
        metric2_sql = (PROJECT_DIR / "sql" / "metric2_type_mix_distribution_unit.sql").read_text(encoding="utf-8")
        metric2_placement_sql = (PROJECT_DIR / "sql" / "metric2_type_placement_mix_distribution_unit.sql").read_text(encoding="utf-8")
        metric3_sql = (PROJECT_DIR / "sql" / "metric3_type_status_distribution_unit.sql").read_text(encoding="utf-8")
        metric3_placement_sql = (PROJECT_DIR / "sql" / "metric3_type_placement_status_distribution_unit.sql").read_text(encoding="utf-8")

        self.assertIn("max_unit_id", metric1_sql)
        self.assertIn("max_unit_id IS NOT NULL", metric1_sql)
        self.assertNotIn("COALESCE(max_unit_id, 'UNKNOWN')", metric1_sql)
        self.assertIn("GROUP BY experiment_group, product, ad_format, max_unit_id", metric1_sql)
        self.assertIn("max_unit_id IS NOT NULL", metric1_placement_sql)
        self.assertIn("max_unit_id IS NOT NULL", metric1_rank_sql)
        self.assertIn("PARTITION BY", metric1_rank_sql)
        self.assertIn("p.max_unit_id", metric1_rank_sql)
        self.assertIn("GROUP BY experiment_group, product, ad_format, max_unit_id, network_cnt", metric2_sql)
        self.assertIn("GROUP BY experiment_group, product, ad_format, max_unit_id, placement_cnt", metric2_placement_sql)
        self.assertIn("GROUP BY experiment_group, product, ad_format, max_unit_id, network_cnt, network_type, type_network_cnt", metric3_sql)
        self.assertIn("GROUP BY experiment_group, product, ad_format, max_unit_id, placement_cnt, network_type, type_placement_cnt", metric3_placement_sql)

    def test_run_ab_dashboard_sql_includes_success_mapping_sql_files(self) -> None:
        """SQL runner 应注册成功 network / placement 新页面需要的两份 SQL。"""
        self.assertEqual(
            runner.SQL_TO_CSV["success_network_by_network_cnt.sql"],
            "success_network_by_network_cnt.csv",
        )
        self.assertEqual(
            runner.SQL_TO_CSV["success_placement_by_placement_cnt.sql"],
            "success_placement_by_placement_cnt.csv",
        )

    def test_success_mapping_payload_builds_cnt_level_success_target_rows(self) -> None:
        """成功映射页应先按全量 cnt 分桶，再在桶内展开成功对象与 fail。"""
        payload = dashboard.build_success_mapping_payload(
            [
                {
                    "experiment_group": dashboard.GROUP_A,
                    "product": "demo",
                    "ad_format": "interstitial",
                    "network_cnt": "3",
                    "success_target": "Google",
                    "request_pv": "6",
                    "denominator_request_pv": "10",
                    "share": "0.6",
                },
                {
                    "experiment_group": dashboard.GROUP_A,
                    "product": "demo",
                    "ad_format": "interstitial",
                    "network_cnt": "3",
                    "success_target": "fail",
                    "request_pv": "4",
                    "denominator_request_pv": "10",
                    "share": "0.4",
                },
                {
                    "experiment_group": dashboard.GROUP_B,
                    "product": "demo",
                    "ad_format": "interstitial",
                    "network_cnt": "3",
                    "success_target": "Pangle",
                    "request_pv": "5",
                    "denominator_request_pv": "8",
                    "share": "0.625",
                },
            ],
            [
                {
                    "experiment_group": dashboard.GROUP_A,
                    "product": "demo",
                    "ad_format": "interstitial",
                    "placement_cnt": "6",
                    "success_target": "pl_1",
                    "request_pv": "7",
                    "denominator_request_pv": "10",
                    "share": "0.7",
                },
                {
                    "experiment_group": dashboard.GROUP_B,
                    "product": "demo",
                    "ad_format": "interstitial",
                    "placement_cnt": "6",
                    "success_target": "fail",
                    "request_pv": "3",
                    "denominator_request_pv": "8",
                    "share": "0.375",
                },
            ],
        )

        combo = payload["combos"]["demo__interstitial"]
        self.assertEqual(combo["network_view"]["count_options"], ["3"])
        self.assertEqual(combo["placement_view"]["count_options"], ["6"])

        network_rows = {
            row["success_target"]: row
            for row in combo["network_view"]["cnt_map"]["3"]["rows"]
        }
        placement_rows = {
            row["success_target"]: row
            for row in combo["placement_view"]["cnt_map"]["6"]["rows"]
        }

        self.assertAlmostEqual(network_rows["Google"]["groups"][dashboard.GROUP_A]["share"], 0.6)
        self.assertAlmostEqual(network_rows["Google"]["groups"][dashboard.GROUP_A]["request_pv"], 6.0)
        self.assertAlmostEqual(network_rows["fail"]["groups"][dashboard.GROUP_A]["share"], 0.4)
        self.assertAlmostEqual(network_rows["Pangle"]["groups"][dashboard.GROUP_B]["share"], 0.625)
        self.assertAlmostEqual(placement_rows["pl_1"]["groups"][dashboard.GROUP_A]["share"], 0.7)
        self.assertAlmostEqual(placement_rows["fail"]["groups"][dashboard.GROUP_B]["share"], 0.375)

    def test_success_mapping_page_script_renders_two_cnt_views(self) -> None:
        """成功映射页脚本应同时渲染 network_cnt 与 placement_cnt 两块，并各自带 cnt selector。"""
        script = dashboard.build_success_mapping_page_script()
        self.assertIn("function renderSuccessMappingPage()", script)
        self.assertIn("成功 network 分布", script)
        self.assertIn("成功 placement 分布", script)
        self.assertIn("network_cnt（单选）", script)
        self.assertIn("placement_cnt（单选）", script)
        self.assertIn("success_target", script)
        self.assertIn("当前 cnt 桶内", script)
        self.assertIn("fail", script)

    def test_success_mapping_html_contains_product_and_format_controls(self) -> None:
        """成功映射页应复用 product / ad_format 选择器，并输出独立 HTML。"""
        html = dashboard.build_success_mapping_html(
            {
                "title": "成功 network / placement 分布",
                "desc": ["先按全量 cnt 分桶，再看桶内成功对象占比。"],
                "products": ["demo"],
                "ad_formats": ["interstitial"],
                "groups": dashboard.GROUP_LABELS,
                "metrics": {
                    "network": {"title": "成功 network 分布", "desc": [], "combos": {}},
                    "placement": {"title": "成功 placement 分布", "desc": [], "combos": {}},
                },
            }
        )
        self.assertIn("成功 network / placement 分布", html)
        self.assertIn("product-select", html)
        self.assertIn("format-select", html)
        self.assertIn("../assets/echarts.min.js", html)
        self.assertIn("ab-success-mapping", html)

    def test_success_mapping_sql_uses_total_cnt_and_fail_bucket(self) -> None:
        """成功映射 SQL 应先按全量 cnt 分桶，再把无成功请求记为 fail。"""
        network_sql = (PROJECT_DIR / "sql" / "success_network_by_network_cnt.sql").read_text(encoding="utf-8")
        placement_sql = (PROJECT_DIR / "sql" / "success_placement_by_placement_cnt.sql").read_text(encoding="utf-8")

        self.assertIn("COUNT(DISTINCT TO_JSON_STRING(STRUCT(network_type, network))) AS network_cnt", network_sql)
        self.assertIn("status = 'AD_LOADED'", network_sql)
        self.assertIn("COALESCE(s.success_target, 'fail') AS success_target", network_sql)
        self.assertIn("COUNT(*) AS placement_cnt", placement_sql)
        self.assertIn("placement_id", placement_sql)
        self.assertIn("status = 'AD_LOADED'", placement_sql)
        self.assertIn("COALESCE(s.success_target, 'fail') AS success_target", placement_sql)

    def test_coverage_metric1_uses_bucket_share(self) -> None:
        """覆盖率分析页的 metric1 应按 req_index 内总请求数计算桶占比。"""
        payload = dashboard.build_coverage_metric1(
            [
                {
                    "experiment_group": dashboard.GROUP_A,
                    "product": "demo",
                    "ad_format": "interstitial",
                    "req_index": "1",
                    "network_cnt": "1",
                    "pv_count": "4",
                },
                {
                    "experiment_group": dashboard.GROUP_A,
                    "product": "demo",
                    "ad_format": "interstitial",
                    "req_index": "1",
                    "network_cnt": "2",
                    "pv_count": "1",
                },
            ]
        )
        combo = payload["combos"]["demo__interstitial"]
        point = combo["groups"][dashboard.GROUP_A]["points"][0]
        self.assertAlmostEqual(point["series"]["1"]["share"], 0.8)
        self.assertAlmostEqual(point["series"]["2"]["share"], 0.2)
        self.assertEqual(point["series"]["1"]["denominator_pv"], 5.0)

    def test_coverage_metric2_uses_bucket_coverage_and_axis_max(self) -> None:
        """覆盖率分析页的 metric2 应按请求覆盖率构建 bidding / waterfall 结果。"""
        payload = dashboard.build_coverage_metric2(
            [
                {
                    "experiment_group": dashboard.GROUP_A,
                    "product": "demo",
                    "ad_format": "interstitial",
                    "req_index": "1",
                    "network_cnt": "2",
                    "network_type": "bidding",
                    "pv_count": "5",
                    "bucket_request_pv": "5",
                    "coverage": "1.0",
                },
                {
                    "experiment_group": dashboard.GROUP_A,
                    "product": "demo",
                    "ad_format": "interstitial",
                    "req_index": "1",
                    "network_cnt": "2",
                    "network_type": "waterfall",
                    "pv_count": "3",
                    "bucket_request_pv": "5",
                    "coverage": "0.6",
                },
            ]
        )
        combo = payload["combos"]["demo__interstitial"]
        cnt_payload = combo["cnt_map"]["2"]
        point = cnt_payload["groups"][dashboard.GROUP_A]["points"][0]
        self.assertEqual(payload["chart_mode"], "coverage")
        self.assertAlmostEqual(point["series"]["bidding"]["share"], 1.0)
        self.assertAlmostEqual(point["series"]["waterfall"]["share"], 0.6)
        self.assertAlmostEqual(cnt_payload["axis_max"], 1.6)

    def test_coverage_metric3_uses_status_coverage_and_axis_max(self) -> None:
        """覆盖率分析页的 metric3 应按请求覆盖率构建 status 结果。"""
        payload = dashboard.build_coverage_metric3(
            [
                {
                    "experiment_group": dashboard.GROUP_A,
                    "product": "demo",
                    "ad_format": "interstitial",
                    "req_index": "1",
                    "network_cnt": "2",
                    "status": "AD_LOADED",
                    "pv_count": "5",
                    "bucket_request_pv": "5",
                    "coverage": "1.0",
                },
                {
                    "experiment_group": dashboard.GROUP_A,
                    "product": "demo",
                    "ad_format": "interstitial",
                    "req_index": "1",
                    "network_cnt": "2",
                    "status": "FAILED_TO_LOAD",
                    "pv_count": "2",
                    "bucket_request_pv": "5",
                    "coverage": "0.4",
                },
            ]
        )
        combo = payload["combos"]["demo__interstitial"]
        cnt_payload = combo["cnt_map"]["2"]
        point = cnt_payload["groups"][dashboard.GROUP_A]["points"][0]
        self.assertEqual(payload["chart_mode"], "coverage")
        self.assertAlmostEqual(point["series"]["AD_LOADED"]["share"], 1.0)
        self.assertAlmostEqual(point["series"]["FAILED_TO_LOAD"]["share"], 0.4)
        self.assertAlmostEqual(cnt_payload["axis_max"], 1.4)

    def test_coverage_metric4_uses_type_request_denominator(self) -> None:
        """覆盖率分析页的 metric4 应按各 network_type 内请求数计算 status 覆盖率。"""
        payload = dashboard.build_coverage_metric4(
            [
                {
                    "experiment_group": dashboard.GROUP_A,
                    "product": "demo",
                    "ad_format": "interstitial",
                    "req_index": "1",
                    "network_cnt": "2",
                    "network_type": "bidding",
                    "status": "AD_LOADED",
                    "pv_count": "2",
                    "type_request_pv": "4",
                    "coverage": "0.5",
                },
                {
                    "experiment_group": dashboard.GROUP_A,
                    "product": "demo",
                    "ad_format": "interstitial",
                    "req_index": "1",
                    "network_cnt": "2",
                    "network_type": "bidding",
                    "status": "FAILED_TO_LOAD",
                    "pv_count": "1",
                    "type_request_pv": "4",
                    "coverage": "0.25",
                },
                {
                    "experiment_group": dashboard.GROUP_A,
                    "product": "demo",
                    "ad_format": "interstitial",
                    "req_index": "1",
                    "network_cnt": "2",
                    "network_type": "waterfall",
                    "status": "FAILED_TO_LOAD",
                    "pv_count": "5",
                    "type_request_pv": "5",
                    "coverage": "1.0",
                },
            ]
        )
        combo = payload["combos"]["demo__interstitial"]
        bidding_payload = combo["cnt_map"]["2"]["type_map"]["bidding"]
        bidding_point = bidding_payload["groups"][dashboard.GROUP_A]["points"][0]
        self.assertEqual(payload["chart_mode"], "coverage")
        self.assertAlmostEqual(bidding_point["series"]["AD_LOADED"]["share"], 0.5)
        self.assertAlmostEqual(bidding_point["series"]["FAILED_TO_LOAD"]["share"], 0.25)
        self.assertEqual(bidding_point["series"]["AD_LOADED"]["denominator_pv"], 4.0)
        self.assertAlmostEqual(bidding_payload["axis_max"], 0.75)

    def test_coverage_page_script_renders_metric4_blocks(self) -> None:
        """覆盖率分析页脚本应新增 metric4，并分别渲染 bidding 与 waterfall 两个块。"""
        script = dashboard.build_coverage_analysis_page_script()
        self.assertIn("function renderMetric4(root,c)", script)
        self.assertIn("metricConfig('metric4')", script)
        self.assertIn("if(c4)renderMetric4(root,c4)", script)
        self.assertIn("bidding 下的 status 覆盖率", script)
        self.assertIn("waterfall 下的 status 覆盖率", script)

    def test_null_bidding_payload_builds_platform_sections_without_banner(self) -> None:
        """null bidding 页面应固定输出 Android/iOS 两个平台，且只保留 interstitial / rewarded。"""
        payload = dashboard.build_null_bidding_payload(
            [
                {
                    "experiment_group": dashboard.GROUP_A,
                    "product": "com.demo.app",
                    "ad_format": "interstitial",
                    "max_unit_id": "unit_a",
                    "ad_unit_name": "Inter P1",
                    "bidding_cnt": "7",
                    "request_pv": "10",
                    "denominator_request_pv": "100",
                    "share": "0.10",
                },
                {
                    "experiment_group": dashboard.GROUP_B,
                    "product": "ios.demo.app",
                    "ad_format": "rewarded",
                    "max_unit_id": "unit_b",
                    "ad_unit_name": "Reward P1",
                    "bidding_cnt": "5",
                    "request_pv": "8",
                    "denominator_request_pv": "80",
                    "share": "0.10",
                },
            ]
        )
        self.assertEqual(payload["platform_order"], ["android", "ios"])
        self.assertEqual(payload["platforms"]["android"]["label"], "Android")
        self.assertEqual(payload["format_order"], ["interstitial", "rewarded"])
        self.assertNotIn("banner", payload["platforms"]["android"]["formats"])
        self.assertFalse(payload["platforms"]["android"]["formats"]["interstitial"]["status_map"]["NULL"]["empty"])
        self.assertNotIn("banner", payload["platforms"]["ios"]["formats"])
        self.assertFalse(payload["platforms"]["ios"]["formats"]["rewarded"]["status_map"]["NULL"]["empty"])
        self.assertIn("A组 = no_is_adx", " ".join(payload["desc"]))
        self.assertIn("B组 = have_is_adx", " ".join(payload["desc"]))
        self.assertIn("NULL = 无效竞价", " ".join(payload["desc"]))

    def test_null_bidding_payload_keeps_all_bidding_cnt_series_and_falls_back_to_unit_id(self) -> None:
        """同一 unit 多行时应保留完整 bidding_cnt 分布，且 ad_unit_name 为空时回退 max_unit_id。"""
        payload = dashboard.build_null_bidding_payload(
            [
                {
                    "experiment_group": dashboard.GROUP_A,
                    "product": "com.demo.app",
                    "ad_format": "interstitial",
                    "max_unit_id": "unit_x",
                    "ad_unit_name": "",
                    "bidding_cnt": "4",
                    "request_pv": "5",
                    "denominator_request_pv": "100",
                    "share": "0.05",
                },
                {
                    "experiment_group": dashboard.GROUP_A,
                    "product": "com.demo.app",
                    "ad_format": "interstitial",
                    "max_unit_id": "unit_x",
                    "ad_unit_name": "",
                    "bidding_cnt": "9",
                    "request_pv": "20",
                    "denominator_request_pv": "100",
                    "share": "0.20",
                },
            ]
        )
        format_payload = payload["platforms"]["android"]["formats"]["interstitial"]
        status_payload = format_payload["status_map"]["NULL"]
        self.assertEqual(status_payload["series_keys"], ["4", "9"])
        point = status_payload["groups"][dashboard.GROUP_A]["points"][0]
        self.assertEqual(point["unit"], "unit_x")
        self.assertAlmostEqual(point["series"]["4"]["share"], 0.05)
        self.assertAlmostEqual(point["series"]["9"]["share"], 0.20)
        self.assertEqual(point["series"]["9"]["request_pv"], 20.0)

    def test_null_bidding_payload_sorts_units_from_p1_to_df(self) -> None:
        """unit 顺序应按 P1..Pn，再到 DF。"""
        payload = dashboard.build_null_bidding_payload(
            [
                {
                    "experiment_group": dashboard.GROUP_A,
                    "product": "com.demo.app",
                    "ad_format": "interstitial",
                    "max_unit_id": "u_df",
                    "ad_unit_name": "Demo Inter df",
                    "bidding_cnt": "4",
                    "request_pv": "5",
                    "denominator_request_pv": "100",
                    "share": "0.05",
                },
                {
                    "experiment_group": dashboard.GROUP_A,
                    "product": "com.demo.app",
                    "ad_format": "interstitial",
                    "max_unit_id": "u_p10",
                    "ad_unit_name": "Demo Inter P10",
                    "bidding_cnt": "4",
                    "request_pv": "5",
                    "denominator_request_pv": "100",
                    "share": "0.05",
                },
                {
                    "experiment_group": dashboard.GROUP_A,
                    "product": "com.demo.app",
                    "ad_format": "interstitial",
                    "max_unit_id": "u_p2",
                    "ad_unit_name": "Demo Inter P2",
                    "bidding_cnt": "4",
                    "request_pv": "5",
                    "denominator_request_pv": "100",
                    "share": "0.05",
                },
            ]
        )
        format_payload = payload["platforms"]["android"]["formats"]["interstitial"]
        self.assertEqual(format_payload["units"], ["Demo Inter P2", "Demo Inter P10", "Demo Inter df"])

    def test_null_bidding_payload_merges_null_and_real_status_rows(self) -> None:
        """null bidding payload 应合并 NULL 与两种真实状态结果，并暴露精简后的 status 选项。"""
        payload = dashboard.build_null_bidding_payload(
            [
                {
                    "experiment_group": dashboard.GROUP_A,
                    "product": "com.demo.app",
                    "ad_format": "interstitial",
                    "max_unit_id": "unit_x",
                    "ad_unit_name": "Demo Inter P1",
                    "status_bucket": "NULL",
                    "bidding_cnt": "0",
                    "request_pv": "95",
                    "denominator_request_pv": "100",
                    "share": "0.95",
                },
                {
                    "experiment_group": dashboard.GROUP_A,
                    "product": "com.demo.app",
                    "ad_format": "interstitial",
                    "max_unit_id": "unit_x",
                    "ad_unit_name": "Demo Inter P1",
                    "bidding_cnt": "4",
                    "request_pv": "5",
                    "denominator_request_pv": "100",
                    "share": "0.05",
                },
            ],
            [
                {
                    "experiment_group": dashboard.GROUP_A,
                    "product": "com.demo.app",
                    "ad_format": "interstitial",
                    "max_unit_id": "unit_x",
                    "status_bucket": "FAILED_TO_LOAD",
                    "bidding_cnt": "0",
                    "request_pv": "88",
                    "denominator_request_pv": "100",
                    "share": "0.88",
                },
                {
                    "experiment_group": dashboard.GROUP_A,
                    "product": "com.demo.app",
                    "ad_format": "interstitial",
                    "max_unit_id": "unit_x",
                    "status_bucket": "FAILED_TO_LOAD",
                    "bidding_cnt": "2",
                    "request_pv": "12",
                    "denominator_request_pv": "100",
                    "share": "0.12",
                },
                {
                    "experiment_group": dashboard.GROUP_B,
                    "product": "com.demo.app",
                    "ad_format": "interstitial",
                    "max_unit_id": "unit_x",
                    "status_bucket": "AD_LOAD_NOT_ATTEMPTED",
                    "bidding_cnt": "0",
                    "request_pv": "108",
                    "denominator_request_pv": "120",
                    "share": "0.90",
                },
                {
                    "experiment_group": dashboard.GROUP_B,
                    "product": "com.demo.app",
                    "ad_format": "interstitial",
                    "max_unit_id": "unit_x",
                    "status_bucket": "AD_LOAD_NOT_ATTEMPTED",
                    "bidding_cnt": "3",
                    "request_pv": "12",
                    "denominator_request_pv": "120",
                    "share": "0.10",
                },
            ],
        )
        self.assertEqual(payload["status_options"], ["NULL", "FAILED_TO_LOAD", "AD_LOAD_NOT_ATTEMPTED"])
        format_payload = payload["platforms"]["android"]["formats"]["interstitial"]
        self.assertIn("NULL", format_payload["status_map"])
        self.assertIn("FAILED_TO_LOAD", format_payload["status_map"])
        self.assertIn("AD_LOAD_NOT_ATTEMPTED", format_payload["status_map"])
        null_point = format_payload["status_map"]["NULL"]["groups"][dashboard.GROUP_A]["points"][0]
        failed_point = format_payload["status_map"]["FAILED_TO_LOAD"]["groups"][dashboard.GROUP_A]["points"][0]
        attempted_point = format_payload["status_map"]["AD_LOAD_NOT_ATTEMPTED"]["groups"][dashboard.GROUP_B]["points"][0]
        self.assertEqual(format_payload["status_map"]["NULL"]["series_keys"], ["0", "4"])
        self.assertEqual(format_payload["status_map"]["FAILED_TO_LOAD"]["series_keys"], ["0", "2"])
        self.assertEqual(format_payload["status_map"]["AD_LOAD_NOT_ATTEMPTED"]["series_keys"], ["0", "3"])
        self.assertAlmostEqual(null_point["series"]["0"]["share"], 0.95)
        self.assertAlmostEqual(null_point["series"]["4"]["share"], 0.05)
        self.assertAlmostEqual(failed_point["series"]["2"]["share"], 0.12)
        self.assertAlmostEqual(attempted_point["series"]["3"]["share"], 0.10)

    def test_null_bidding_payload_builds_group_level_pie_breakdown(self) -> None:
        """右侧饼图应展示当前状态与实验组下，各 unit 的 request 占比。"""
        payload = dashboard.build_null_bidding_payload(
            [
                {
                    "experiment_group": dashboard.GROUP_A,
                    "product": "com.demo.app",
                    "ad_format": "interstitial",
                    "max_unit_id": "u_p1",
                    "ad_unit_name": "Demo Inter P1",
                    "status_bucket": "NULL",
                    "bidding_cnt": "0",
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
                    "status_bucket": "NULL",
                    "bidding_cnt": "0",
                    "request_pv": "180",
                    "denominator_request_pv": "200",
                    "share": "0.90",
                },
            ]
        )
        pie = payload["platforms"]["android"]["formats"]["interstitial"]["status_map"]["NULL"]["groups"][dashboard.GROUP_A]["pie"]
        self.assertEqual(pie["total_request_pv"], 300.0)
        self.assertEqual([item["unit"] for item in pie["items"]], ["Demo Inter P1", "Demo Inter P2"])
        self.assertAlmostEqual(pie["items"][0]["request_pv"], 100.0)
        self.assertAlmostEqual(pie["items"][0]["share"], 1 / 3)
        self.assertAlmostEqual(pie["items"][1]["request_pv"], 200.0)
        self.assertAlmostEqual(pie["items"][1]["share"], 2 / 3)

    def test_null_bidding_page_script_keeps_existing_hover_style_with_status_selector(self) -> None:
        """null bidding 单页应保留现有 tooltip 跟手交互，并新增 status 单选筛选。"""
        script = dashboard.build_null_bidding_page_script()
        self.assertIn("trigger:'axis'", script)
        self.assertIn("window._current_mouse_y", script)
        self.assertIn("chart.getZr().on('mousemove'", script)
        self.assertIn("status-select", script)
        self.assertIn("renderStatusControl", script)
        self.assertIn("currentStatus", script)
        self.assertIn("function pieOption", script)
        self.assertIn("unit-pie-chart", script)
        self.assertIn("renderGroupRow", script)
        self.assertIn("A组（no_is_adx）", script)
        self.assertIn("B组（have_is_adx）", script)
        self.assertIn("总请求量", script)
        self.assertIn("占比", script)
        self.assertIn("cnt", script)
        self.assertIn("pv", script)
        self.assertIn("top:0", script)
        self.assertIn("shortUnitLabel", script)
        self.assertNotIn("AD_LOADED", script)
        self.assertNotIn("metric-table", script)
        self.assertNotIn("<table", script)
        self.assertNotIn("hover 查看 share / request_pv / bidding_cnt", script)

    def test_null_bidding_html_contains_platform_sections_and_full_width_format_cards(self) -> None:
        """null bidding 页面应渲染 Android/iOS 与 interstitial/rewarded 两类纵向全宽卡片。"""
        payload = {
            "title": "Null Bidding Unit Share",
            "desc": ["A组 = no_is_adx", "B组 = have_is_adx", "NULL = 无效竞价", "cnt = 无效竞价渠道数；pv = 去重请求量"],
            "format_order": ["interstitial", "rewarded"],
            "status_options": ["NULL", "FAILED_TO_LOAD", "AD_LOAD_NOT_ATTEMPTED"],
            "platform_order": ["android", "ios"],
            "platforms": {
                "android": {
                    "label": "Android",
                    "formats": {
                        "interstitial": {"label": "interstitial", "empty": False, "status_map": {"NULL": {"groups": {dashboard.GROUP_A: {"points": [], "pie": {"items": [], "total_request_pv": 0}}, dashboard.GROUP_B: {"points": [], "pie": {"items": [], "total_request_pv": 0}}}}}},
                        "rewarded": {"label": "rewarded", "empty": False, "status_map": {"NULL": {"groups": {dashboard.GROUP_A: {"points": [], "pie": {"items": [], "total_request_pv": 0}}, dashboard.GROUP_B: {"points": [], "pie": {"items": [], "total_request_pv": 0}}}}}},
                    },
                },
                "ios": {
                    "label": "iOS",
                    "formats": {
                        "interstitial": {"label": "interstitial", "empty": False, "status_map": {"NULL": {"groups": {dashboard.GROUP_A: {"points": [], "pie": {"items": [], "total_request_pv": 0}}, dashboard.GROUP_B: {"points": [], "pie": {"items": [], "total_request_pv": 0}}}}}},
                        "rewarded": {"label": "rewarded", "empty": False, "status_map": {"NULL": {"groups": {dashboard.GROUP_A: {"points": [], "pie": {"items": [], "total_request_pv": 0}}, dashboard.GROUP_B: {"points": [], "pie": {"items": [], "total_request_pv": 0}}}}}},
                    },
                },
            },
        }
        html = dashboard.build_null_bidding_html(payload)
        self.assertIn("Android", html)
        self.assertIn("iOS", html)
        self.assertIn("interstitial", html)
        self.assertIn("rewarded", html)
        self.assertIn("null-format-stack", html)
        self.assertIn("status-select", html)
        self.assertIn("unit-pie-chart", html)
        self.assertIn("chart-row", html)
        self.assertIn("A组 = no_is_adx", html)
        self.assertIn("NULL = 无效竞价", html)
        self.assertIn(".chart{height:320px}", html)
        self.assertIn("max-width:1680px", html)
        self.assertNotIn("Android 在上", html)
        self.assertNotIn("AD_LOADED", html)
        self.assertNotIn("banner", html)
        self.assertIn("../assets/echarts.min.js", html)

    def test_bidding_network_status_payload_builds_block_level_readable_unit_selector(self) -> None:
        """新 network status 页面应按 platform+format 暴露各自可读 unit 选择器。"""
        payload = dashboard.build_bidding_network_status_payload(
            [
                {
                    "experiment_group": dashboard.GROUP_A,
                    "product": "com.demo.app",
                    "ad_format": "interstitial",
                    "max_unit_id": "u_p2",
                    "ad_unit_name": "Demo Inter P2",
                    "network_type": "bidding",
                    "network": "Pangle",
                    "status_bucket": "NULL",
                    "request_pv": "60",
                    "denominator_request_pv": "100",
                    "share": "0.60",
                },
                {
                    "experiment_group": dashboard.GROUP_B,
                    "product": "ios.demo.app",
                    "ad_format": "rewarded",
                    "max_unit_id": "u_df",
                    "ad_unit_name": "Demo Reward df",
                    "network_type": "waterfall",
                    "network": "Mintegral",
                    "status_bucket": "AD_LOADED",
                    "request_pv": "20",
                    "denominator_request_pv": "80",
                    "share": "0.25",
                },
            ]
        )
        self.assertEqual(payload["platform_order"], ["android", "ios"])
        self.assertEqual(payload["format_order"], ["interstitial", "rewarded"])
        self.assertEqual(payload["network_type_order"], ["bidding", "waterfall"])
        android_inter = payload["platforms"]["android"]["formats"]["interstitial"]
        ios_reward = payload["platforms"]["ios"]["formats"]["rewarded"]
        self.assertEqual(
            android_inter["unit_options"],
            [
                {"value": "u_p2", "label": "Demo Inter P2"},
            ],
        )
        self.assertEqual(android_inter["default_unit"], "u_p2")
        self.assertEqual(
            ios_reward["unit_options"],
            [
                {"value": "u_df", "label": "Demo Reward df"},
            ],
        )
        self.assertEqual(ios_reward["default_unit"], "u_df")
        self.assertNotIn("unit_options", payload)
        self.assertNotIn("default_unit", payload)

    def test_bidding_network_status_payload_splits_bidding_and_waterfall_blocks(self) -> None:
        """同一 unit 下应按 network_type 分开渲染 bidding 和 waterfall。"""
        payload = dashboard.build_bidding_network_status_payload(
            [
                {
                    "experiment_group": dashboard.GROUP_A,
                    "product": "com.demo.app",
                    "ad_format": "interstitial",
                    "max_unit_id": "u_p1",
                    "ad_unit_name": "Demo Inter P1",
                    "network_type": "bidding",
                    "network": "Google",
                    "status_bucket": "NULL",
                    "request_pv": "40",
                    "denominator_request_pv": "100",
                    "share": "0.40",
                },
                {
                    "experiment_group": dashboard.GROUP_A,
                    "product": "com.demo.app",
                    "ad_format": "interstitial",
                    "max_unit_id": "u_p1",
                    "ad_unit_name": "Demo Inter P1",
                    "network_type": "bidding",
                    "network": "Google",
                    "status_bucket": "AD_LOADED",
                    "request_pv": "10",
                    "denominator_request_pv": "100",
                    "share": "0.10",
                },
                {
                    "experiment_group": dashboard.GROUP_B,
                    "product": "com.demo.app",
                    "ad_format": "interstitial",
                    "max_unit_id": "u_p1",
                    "ad_unit_name": "Demo Inter P1",
                    "network_type": "bidding",
                    "network": "Pangle",
                    "status_bucket": "FAILED_TO_LOAD",
                    "request_pv": "30",
                    "denominator_request_pv": "120",
                    "share": "0.25",
                },
                {
                    "experiment_group": dashboard.GROUP_B,
                    "product": "com.demo.app",
                    "ad_format": "interstitial",
                    "max_unit_id": "u_p1",
                    "ad_unit_name": "Demo Inter P1",
                    "network_type": "bidding",
                    "network": "Pangle",
                    "status_bucket": "AD_LOAD_NOT_ATTEMPTED",
                    "request_pv": "20",
                    "denominator_request_pv": "120",
                    "share": "0.1667",
                },
                {
                    "experiment_group": dashboard.GROUP_A,
                    "product": "com.demo.app",
                    "ad_format": "interstitial",
                    "max_unit_id": "u_p1",
                    "ad_unit_name": "Demo Inter P1",
                    "network_type": "waterfall",
                    "network": "AdMob",
                    "status_bucket": "AD_LOADED",
                    "request_pv": "70",
                    "denominator_request_pv": "100",
                    "share": "0.70",
                },
                {
                    "experiment_group": dashboard.GROUP_A,
                    "product": "com.demo.app",
                    "ad_format": "interstitial",
                    "max_unit_id": "u_p1",
                    "ad_unit_name": "Demo Inter P1",
                    "network_type": "bidding",
                    "network": "Pangle",
                    "status_bucket": "NULL",
                    "request_pv": "5",
                    "denominator_request_pv": "100",
                    "share": "0.05",
                },
            ]
        )
        unit_payload = payload["platforms"]["android"]["formats"]["interstitial"]["unit_map"]["u_p1"]
        bidding_block = unit_payload["network_types"]["bidding"]
        waterfall_block = unit_payload["network_types"]["waterfall"]
        self.assertEqual(bidding_block["networks"], ["Pangle", "Google"])
        self.assertEqual(waterfall_block["networks"], ["AdMob"])
        self.assertEqual(bidding_block["status_order"], ["AD_LOADED", "FAILED_TO_LOAD", "AD_LOAD_NOT_ATTEMPTED", "NULL"])
        self.assertAlmostEqual(bidding_block["groups"][dashboard.GROUP_A]["series"]["NULL"][0]["share"], 0.05)
        self.assertAlmostEqual(bidding_block["groups"][dashboard.GROUP_A]["series"]["NULL"][1]["share"], 0.40)
        self.assertAlmostEqual(bidding_block["groups"][dashboard.GROUP_A]["series"]["AD_LOADED"][1]["share"], 0.10)
        self.assertAlmostEqual(bidding_block["groups"][dashboard.GROUP_B]["series"]["FAILED_TO_LOAD"][0]["share"], 0.25)
        self.assertAlmostEqual(bidding_block["groups"][dashboard.GROUP_B]["series"]["AD_LOAD_NOT_ATTEMPTED"][0]["share"], 0.1667)
        self.assertAlmostEqual(waterfall_block["groups"][dashboard.GROUP_A]["series"]["AD_LOADED"][0]["share"], 0.70)

    def test_bidding_network_status_payload_keeps_all_status_series_from_sql(self) -> None:
        """同一 type+network 的多状态结果已由 SQL 聚合好，payload 不应再次压缩。"""
        payload = dashboard.build_bidding_network_status_payload(
            [
                {
                    "experiment_group": dashboard.GROUP_A,
                    "product": "com.demo.app",
                    "ad_format": "rewarded",
                    "max_unit_id": "u_r1",
                    "ad_unit_name": "Demo Reward P1",
                    "network_type": "waterfall",
                    "network": "AdMob",
                    "status_bucket": "FAILED_TO_LOAD",
                    "request_pv": "30",
                    "denominator_request_pv": "100",
                    "share": "0.30",
                },
                {
                    "experiment_group": dashboard.GROUP_A,
                    "product": "com.demo.app",
                    "ad_format": "rewarded",
                    "max_unit_id": "u_r1",
                    "ad_unit_name": "Demo Reward P1",
                    "network_type": "waterfall",
                    "network": "AdMob",
                    "status_bucket": "AD_LOADED",
                    "request_pv": "10",
                    "denominator_request_pv": "100",
                    "share": "0.10",
                },
            ]
        )
        waterfall_block = payload["platforms"]["android"]["formats"]["rewarded"]["unit_map"]["u_r1"]["network_types"]["waterfall"]
        self.assertAlmostEqual(waterfall_block["groups"][dashboard.GROUP_A]["series"]["AD_LOADED"][0]["share"], 0.10)
        self.assertAlmostEqual(waterfall_block["groups"][dashboard.GROUP_A]["series"]["FAILED_TO_LOAD"][0]["share"], 0.30)

    def test_bidding_network_status_page_script_uses_status_color_and_group_line_style(self) -> None:
        """新页面应使用块级 unit 单选、status 颜色和 A/B 线型区分。"""
        script = dashboard.build_bidding_network_status_page_script()
        self.assertIn("network-status-unit-select", script)
        self.assertNotIn("status-select", script)
        self.assertNotIn("platform-select", script)
        self.assertNotIn("format-select", script)
        self.assertIn("lineStyle", script)
        self.assertIn("dashed", script)
        self.assertIn("solid", script)
        self.assertIn("AD_LOADED", script)
        self.assertIn("FAILED_TO_LOAD", script)
        self.assertIn("AD_LOAD_NOT_ATTEMPTED", script)
        self.assertIn("NULL", script)
        self.assertIn("renderUnitSelector", script)
        self.assertIn("renderFormatBlockBody", script)
        self.assertIn("formatPayload.default_unit", script)
        self.assertIn("trigger:'axis'", script)
        self.assertIn("containLabel:true", script)
        self.assertIn("bottom:86", script)
        self.assertIn("right:20", script)
        self.assertIn("function formatNetworkAxisLabel", script)
        self.assertIn("function calculateVisibleNetworkCount", script)
        self.assertIn("type:'slider'", script)
        self.assertIn("type:'inside'", script)
        self.assertIn("chartEl.style.width='100%'", script)
        self.assertIn("const applyResponsiveOption", script)
        self.assertIn("const showAll = networkCount <= effectiveVisibleCount", script)
        self.assertIn("new ResizeObserver(()=>applyResponsiveOption())", script)
        self.assertIn("NETWORK_TYPE_ORDER", script)
        self.assertIn("renderNetworkTypeBlock", script)
        self.assertIn("bidding", script)
        self.assertIn("waterfall", script)

    def test_bidding_network_status_html_contains_block_level_unit_selectors_and_four_blocks(self) -> None:
        """新页面应在每个块内包含 unit 选择器，并固定渲染四个 platform/format 块。"""
        payload = {
            "title": "Bidding Network Status Share",
            "desc": ["A组 = no_is_adx", "B组 = have_is_adx", "颜色表示 status，实线/虚线表示组别。"],
            "platform_order": ["android", "ios"],
            "format_order": ["interstitial", "rewarded"],
            "network_type_order": ["bidding", "waterfall"],
            "platforms": {
                "android": {
                    "label": "Android",
                    "formats": {
                        "interstitial": {
                            "label": "interstitial",
                            "unit_options": [{"value": "u_p1", "label": "Demo Inter P1"}],
                            "default_unit": "u_p1",
                            "unit_map": {"u_p1": {"label": "Demo Inter P1", "network_types": {}}},
                        },
                        "rewarded": {
                            "label": "rewarded",
                            "unit_options": [{"value": "u_r1", "label": "Demo Reward P1"}],
                            "default_unit": "u_r1",
                            "unit_map": {"u_r1": {"label": "Demo Reward P1", "network_types": {}}},
                        },
                    },
                },
                "ios": {
                    "label": "iOS",
                    "formats": {
                        "interstitial": {
                            "label": "interstitial",
                            "unit_options": [{"value": "u_i1", "label": "iOS Inter P1"}],
                            "default_unit": "u_i1",
                            "unit_map": {"u_i1": {"label": "iOS Inter P1", "network_types": {}}},
                        },
                        "rewarded": {
                            "label": "rewarded",
                            "unit_options": [{"value": "u_ir1", "label": "iOS Reward df"}],
                            "default_unit": "u_ir1",
                            "unit_map": {"u_ir1": {"label": "iOS Reward df", "network_types": {}}},
                        },
                    },
                },
            },
            "groups": dashboard.NULL_BIDDING_GROUP_LABELS,
            "status_order": ["NULL", "AD_LOADED", "FAILED_TO_LOAD", "AD_LOAD_NOT_ATTEMPTED"],
        }
        html = dashboard.build_bidding_network_status_html(payload)
        self.assertNotIn('id="unit-select"', html)
        self.assertNotIn("status-select", html)
        self.assertNotIn("platform-select", html)
        self.assertNotIn("format-select", html)
        self.assertIn("Android", html)
        self.assertIn("iOS", html)
        self.assertIn("interstitial", html)
        self.assertIn("rewarded", html)
        self.assertIn("network-status-grid", html)
        self.assertIn("bidding", html)
        self.assertIn("waterfall", html)
        self.assertIn("颜色表示 status", html)
        self.assertIn("../assets/echarts.min.js", html)

    def test_bidding_network_status_html_avoids_global_unit_selector(self) -> None:
        """新页面不应再输出顶部统一 unit selector 的 DOM。"""
        payload = {
            "title": "Bidding Network Status Share",
            "desc": ["A组 = no_is_adx", "B组 = have_is_adx"],
            "platform_order": ["android"],
            "format_order": ["interstitial"],
            "network_type_order": ["bidding", "waterfall"],
            "platforms": {
                "android": {
                    "label": "Android",
                    "formats": {
                        "interstitial": {
                            "label": "interstitial",
                            "unit_options": [{"value": "u_p1", "label": "Demo Inter P1"}],
                            "default_unit": "u_p1",
                            "unit_map": {"u_p1": {"label": "Demo Inter P1", "network_types": {}}},
                        },
                    },
                },
            },
            "groups": dashboard.NULL_BIDDING_GROUP_LABELS,
            "status_order": ["NULL", "AD_LOADED", "FAILED_TO_LOAD", "AD_LOAD_NOT_ATTEMPTED"],
        }
        html = dashboard.build_bidding_network_status_html(payload)
        self.assertNotIn('<section class="controls">', html)


if __name__ == "__main__":
    unittest.main()
