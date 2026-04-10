"""ad_kill 用户分布独立 HTML 的回归测试。"""

from __future__ import annotations

import csv
import importlib.util
import types
from pathlib import Path
import shutil
import unittest
from unittest import mock


PROJECT_DIR = Path(__file__).resolve().parents[1]
SCRIPT_PATH = PROJECT_DIR / "scripts" / "run_user_distribution_dashboard.py"
TEST_TMP_DIR = PROJECT_DIR / "tests_tmp"


def load_module():
    """按需加载脚本模块，便于测试尚未安装为包的脚本。"""
    spec = importlib.util.spec_from_file_location(
        "run_user_distribution_dashboard", SCRIPT_PATH
    )
    module = importlib.util.module_from_spec(spec)
    assert spec is not None and spec.loader is not None
    spec.loader.exec_module(module)
    return module


class WorkspaceTempDir:
    """在工作区内创建临时目录，避免写入系统 temp。"""

    def __init__(self, name: str):
        self.name = name

    def __enter__(self) -> Path:
        TEST_TMP_DIR.mkdir(exist_ok=True)
        self.path = TEST_TMP_DIR / self.name
        if self.path.exists():
            shutil.rmtree(self.path, ignore_errors=True)
        self.path.mkdir(parents=True, exist_ok=True)
        return self.path

    def __exit__(self, exc_type, exc, tb) -> None:
        shutil.rmtree(self.path, ignore_errors=True)


class UserDistributionDashboardTests(unittest.TestCase):
    """覆盖 CSV 解析、结构构建与 HTML 生成。"""

    def test_script_exists(self) -> None:
        """实现应新增独立 runner 脚本。"""
        self.assertTrue(SCRIPT_PATH.exists())

    def test_read_csv_rows_strips_bom_from_header(self) -> None:
        """CSV 读取应兼容 UTF-8 BOM 表头。"""
        module = load_module()
        with WorkspaceTempDir("read_bom") as temp_dir:
            csv_path = temp_dir / "bom.csv"
            csv_path.write_text("\ufeffproduct,ab_group\nball_sort,A\n", encoding="utf-8")

            rows = module.read_csv_rows(csv_path)

        self.assertEqual(rows, [{"product": "ball_sort", "ab_group": "A"}])

    def test_build_data_structures_group_expected_fields(self) -> None:
        """前端数据结构应按方案拆成 composition 与 scene 两块。"""
        module = load_module()
        composition_rows = [
            {
                "product": "ball_sort",
                "event_date": "2026-04-01",
                "ab_group": "A",
                "total_dau": "100",
                "user_type": "new",
                "type_dau": "60",
                "type_ratio": "0.6",
            },
            {
                "product": "ball_sort",
                "event_date": "2026-04-01",
                "ab_group": "A",
                "total_dau": "100",
                "user_type": "old",
                "type_dau": "40",
                "type_ratio": "0.4",
            },
            {
                "product": "ball_sort",
                "event_date": "2026-04-01",
                "ab_group": "B",
                "total_dau": "120",
                "user_type": "new",
                "type_dau": "72",
                "type_ratio": "0.6",
            },
            {
                "product": "ball_sort",
                "event_date": "2026-04-01",
                "ab_group": "B",
                "total_dau": "120",
                "user_type": "old",
                "type_dau": "48",
                "type_ratio": "0.4",
            },
        ]
        scene_rows = [
            {
                "view_type": "overall",
                "product": "ball_sort",
                "ab_group": "A",
                "user_type": "",
                "total_users": "80",
                "long_kill_users": "12",
                "long_kill_ratio": "0.15",
                "short_kill_users": "8",
                "short_kill_ratio": "0.1",
                "any_kill_users": "20",
                "any_kill_ratio": "0.25",
            },
            {
                "view_type": "by_user_type",
                "product": "ball_sort",
                "ab_group": "A",
                "user_type": "new",
                "total_users": "50",
                "long_kill_users": "10",
                "long_kill_ratio": "0.2",
                "short_kill_users": "5",
                "short_kill_ratio": "0.1",
                "any_kill_users": "15",
                "any_kill_ratio": "0.3",
            },
        ]

        composition = module.build_composition_data(composition_rows)
        scene = module.build_scene_data(scene_rows)

        self.assertEqual(composition["ball_sort"]["2026-04-01"]["A"]["new_dau"], 60)
        self.assertEqual(composition["ball_sort"]["2026-04-01"]["A"]["old_ratio"], 0.4)
        self.assertEqual(composition["ball_sort"]["2026-04-01"]["B"]["total_dau"], 120)
        self.assertEqual(scene["ball_sort"]["overall"]["A"]["any_kill_ratio"], 0.25)
        self.assertEqual(scene["ball_sort"]["by_user_type"]["new"]["A"]["total_users"], 50)

    def test_resolve_bq_command_prefers_bq_cmd_on_windows(self) -> None:
        """Windows 子进程应优先使用 bq.cmd，避免直接执行 bq 失败。"""
        module = load_module()
        bq_cmd = r"C:\tooling\google-cloud-sdk\bin\bq.cmd"
        with mock.patch.object(module.shutil, "which", side_effect=[bq_cmd, None]):
            command = module.resolve_bq_command()

        self.assertEqual(command, [bq_cmd])

    def test_run_bq_query_falls_back_to_bigquery_client_when_cli_is_broken(self) -> None:
        """当 bq CLI 在当前环境依赖损坏时，应自动回退到 Python BigQuery 客户端。"""
        module = load_module()
        with WorkspaceTempDir("client_fallback") as temp_dir:
            sql_path = temp_dir / "demo.sql"
            csv_path = temp_dir / "demo.csv"
            sql_path.write_text("select 1 as ok", encoding="utf-8")

            broken_result = types.SimpleNamespace(
                returncode=1,
                stdout="",
                stderr="AttributeError: module 'absl.flags' has no attribute 'FLAGS'",
            )
            with mock.patch.object(module.subprocess, "run", return_value=broken_result):
                with mock.patch.object(
                    module,
                    "query_to_csv_via_bigquery_client",
                    side_effect=lambda sql_text, out_path: out_path.write_text(
                        "ok\n1\n", encoding="utf-8"
                    ),
                ) as client_fallback:
                    module.run_bq_query(sql_path, csv_path)

            client_fallback.assert_called_once()
            self.assertEqual(csv_path.read_text(encoding="utf-8"), "ok\n1\n")

    def test_main_skip_query_writes_single_html(self) -> None:
        """跳过查询时应直接从 CSV 渲染独立 HTML。"""
        module = load_module()
        with WorkspaceTempDir("skip_query_html") as temp_root:
            composition_csv = temp_root / "ad_kill_dau_user_composition.csv"
            scene_csv = temp_root / "ad_kill_scene_user_analysis.csv"
            html_path = temp_root / "ad_kill_user_distribution.html"

            with composition_csv.open("w", encoding="utf-8-sig", newline="") as file_obj:
                writer = csv.DictWriter(
                    file_obj,
                    fieldnames=[
                        "product",
                        "event_date",
                        "ab_group",
                        "total_dau",
                        "user_type",
                        "type_dau",
                        "type_ratio",
                    ],
                )
                writer.writeheader()
                writer.writerows(
                    [
                        {
                            "product": "ball_sort",
                            "event_date": "2026-04-01",
                            "ab_group": "A",
                            "total_dau": "100",
                            "user_type": "new",
                            "type_dau": "60",
                            "type_ratio": "0.6",
                        },
                        {
                            "product": "ball_sort",
                            "event_date": "2026-04-01",
                            "ab_group": "A",
                            "total_dau": "100",
                            "user_type": "old",
                            "type_dau": "40",
                            "type_ratio": "0.4",
                        },
                        {
                            "product": "ball_sort",
                            "event_date": "2026-04-01",
                            "ab_group": "B",
                            "total_dau": "120",
                            "user_type": "new",
                            "type_dau": "72",
                            "type_ratio": "0.6",
                        },
                        {
                            "product": "ball_sort",
                            "event_date": "2026-04-01",
                            "ab_group": "B",
                            "total_dau": "120",
                            "user_type": "old",
                            "type_dau": "48",
                            "type_ratio": "0.4",
                        },
                    ]
                )

            with scene_csv.open("w", encoding="utf-8-sig", newline="") as file_obj:
                writer = csv.DictWriter(
                    file_obj,
                    fieldnames=[
                        "view_type",
                        "product",
                        "ab_group",
                        "user_type",
                        "total_users",
                        "long_kill_users",
                        "long_kill_ratio",
                        "short_kill_users",
                        "short_kill_ratio",
                        "any_kill_users",
                        "any_kill_ratio",
                    ],
                )
                writer.writeheader()
                writer.writerows(
                    [
                        {
                            "view_type": "overall",
                            "product": "ball_sort",
                            "ab_group": "A",
                            "user_type": "",
                            "total_users": "80",
                            "long_kill_users": "12",
                            "long_kill_ratio": "0.15",
                            "short_kill_users": "8",
                            "short_kill_ratio": "0.10",
                            "any_kill_users": "20",
                            "any_kill_ratio": "0.25",
                        },
                        {
                            "view_type": "overall",
                            "product": "ball_sort",
                            "ab_group": "B",
                            "user_type": "",
                            "total_users": "90",
                            "long_kill_users": "18",
                            "long_kill_ratio": "0.20",
                            "short_kill_users": "9",
                            "short_kill_ratio": "0.10",
                            "any_kill_users": "27",
                            "any_kill_ratio": "0.30",
                        },
                        {
                            "view_type": "by_user_type",
                            "product": "ball_sort",
                            "ab_group": "A",
                            "user_type": "new",
                            "total_users": "50",
                            "long_kill_users": "10",
                            "long_kill_ratio": "0.20",
                            "short_kill_users": "5",
                            "short_kill_ratio": "0.10",
                            "any_kill_users": "15",
                            "any_kill_ratio": "0.30",
                        },
                    ]
                )

            exit_code = module.main(
                [
                    "--skip-query",
                    "--composition-csv",
                    str(composition_csv),
                    "--scene-csv",
                    str(scene_csv),
                    "--html",
                    str(html_path),
                ]
            )

            self.assertEqual(exit_code, 0)
            self.assertTrue(html_path.exists())
            html_text = html_path.read_text(encoding="utf-8")
            self.assertIn("Ad Kill 用户分布", html_text)
            self.assertIn("新老用户构成", html_text)
            self.assertIn("ad_kill_scene 分布", html_text)


if __name__ == "__main__":
    unittest.main()
