"""项目分组与乱码修复回归测试。"""

from __future__ import annotations

from pathlib import Path
import unittest


REPO_ROOT = Path(__file__).resolve().parents[1]
LATENCY_DIR = REPO_ROOT / "projects" / "latency_analysis"
REACH_RATE_DIR = REPO_ROOT / "projects" / "reach_rate_analysis"


class ProjectLayoutTests(unittest.TestCase):
    """校验项目目录落位与中文注释修复。"""

    def test_latency_analysis_contains_network_distribution_files(self) -> None:
        """网络状态分布分析应并入 latency_analysis。"""
        self.assertTrue((LATENCY_DIR / "scripts" / "run_latency_request_network_distribution.py").exists())
        self.assertTrue((LATENCY_DIR / "sql" / "latency_request_network_distribution.sql").exists())

    def test_reach_rate_analysis_contains_merged_files(self) -> None:
        """reach_rate 与 request_latency_unmatched 应放在同一项目目录。"""
        self.assertTrue((REACH_RATE_DIR / "scripts" / "run_isadx_gap_analysis.py").exists())
        self.assertTrue((REACH_RATE_DIR / "scripts" / "run_b_group_request_latency_unmatched_analysis.py").exists())
        self.assertTrue((REACH_RATE_DIR / "sql" / "reach_rate_ab_group_compare.sql").exists())
        self.assertTrue((REACH_RATE_DIR / "sql" / "b_group_request_latency_unmatched_summary.sql").exists())

    def test_garbled_comments_are_rewritten_in_reach_rate_project(self) -> None:
        """受损中文注释应恢复为可读中文。"""
        targets = [
            REPO_ROOT / "projects" / "reach_rate_analysis" / "scripts" / "run_isadx_gap_analysis.py",
            REPO_ROOT / "projects" / "reach_rate_analysis" / "scripts" / "run_b_group_request_latency_unmatched_analysis.py",
            REPO_ROOT / "projects" / "reach_rate_analysis" / "sql" / "reach_rate_ab_group_compare.sql",
            REPO_ROOT / "projects" / "reach_rate_analysis" / "sql" / "b_group_request_latency_unmatched_summary.sql",
        ]
        for path in targets:
            text = path.read_text(encoding="utf-8")
            self.assertNotIn("???", text, msg=str(path))
            self.assertNotIn("��", text, msg=str(path))


if __name__ == "__main__":
    unittest.main()
