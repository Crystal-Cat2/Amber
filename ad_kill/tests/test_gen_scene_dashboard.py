"""ad_kill scene dashboard 生成脚本回归测试。"""

from __future__ import annotations

from pathlib import Path
import unittest


PROJECT_DIR = Path(__file__).resolve().parents[1]
SCRIPT_PATH = PROJECT_DIR / "scripts" / "gen_scene_dashboard.py"


class GenSceneDashboardScriptTests(unittest.TestCase):
    """校验 dashboard 脚本已切到新的 6 类 scene，并移除 long kill 分布图。"""

    def test_script_uses_new_scene_groups_and_removes_lw_chart(self) -> None:
        self.assertTrue(SCRIPT_PATH.exists())
        script_text = SCRIPT_PATH.read_text(encoding="utf-8")

        self.assertIn("'long': 'rgb(239, 68, 68)'", script_text)
        self.assertIn("'short': 'rgb(245, 158, 11)'", script_text)
        self.assertIn("'other_0':", script_text)
        self.assertIn("'other_1':", script_text)
        self.assertIn("'other_2':", script_text)
        self.assertIn("'other_3plus':", script_text)
        self.assertIn("const scenes = ['long', 'short', 'other_0', 'other_1', 'other_2', 'other_3plus'];", script_text)
        self.assertNotIn("chartLwKillCount", script_text)
        self.assertNotIn("renderLwKillCountBar", script_text)
        self.assertNotIn("long_watch_kill 用户杀广告次数分布", script_text)


if __name__ == "__main__":
    unittest.main()
