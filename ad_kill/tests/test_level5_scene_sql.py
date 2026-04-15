"""第5关 scene 分组 SQL 回归测试。"""

from __future__ import annotations

from pathlib import Path
import unittest


PROJECT_DIR = Path(__file__).resolve().parents[1]
DIST_SQL = PROJECT_DIR / "sql" / "ad_kill_level5_scene_distribution.sql"
SURVIVAL_SQL = PROJECT_DIR / "sql" / "ad_kill_scene_level_survival.sql"


class Level5SceneSqlTests(unittest.TestCase):
    """校验第5关 scene 分组切换到 game_new_start 先出现优先口径。"""

    def test_distribution_sql_uses_first_seen_gns_classification(self) -> None:
        self.assertTrue(DIST_SQL.exists())
        sql_text = DIST_SQL.read_text(encoding="utf-8")

        self.assertIn("event_name = 'game_new_start'", sql_text)
        self.assertNotIn("event_name IN ('game_new_start', 'game_win')", sql_text)
        self.assertIn("first_long_ts", sql_text)
        self.assertIn("first_short_ts", sql_text)
        self.assertIn("WHEN first_long_ts IS NOT NULL AND first_short_ts IS NOT NULL", sql_text)
        self.assertIn("WHEN first_long_ts < first_short_ts THEN 'long'", sql_text)
        self.assertIn("WHEN first_short_ts < first_long_ts THEN 'short'", sql_text)
        self.assertIn("ELSE 'long'", sql_text)
        self.assertIn("THEN 'other_0'", sql_text)
        self.assertIn("THEN 'other_1'", sql_text)
        self.assertIn("THEN 'other_2'", sql_text)
        self.assertIn("ELSE 'other_3plus'", sql_text)
        self.assertIn("scene_group", sql_text)
        self.assertIn("COUNT(DISTINCT user_pseudo_id) AS uv", sql_text)
        self.assertIn("SAFE_DIVIDE(s.uv, d.dau)", sql_text)
        self.assertIn("event_name = 'lib_fullscreen_ad_killed'", sql_text)
        self.assertIn("WHERE ep.key = 'activity_id') = 0", sql_text)

    def test_survival_sql_uses_same_scene_classification(self) -> None:
        self.assertTrue(SURVIVAL_SQL.exists())
        sql_text = SURVIVAL_SQL.read_text(encoding="utf-8")

        self.assertIn("event_name = 'game_new_start'", sql_text)
        self.assertNotIn("event_name IN ('game_new_start', 'game_win')", sql_text)
        self.assertIn("first_long_ts", sql_text)
        self.assertIn("first_short_ts", sql_text)
        self.assertIn("WHEN first_long_ts IS NOT NULL AND first_short_ts IS NOT NULL", sql_text)
        self.assertIn("scene_group", sql_text)
        self.assertIn("other_0", sql_text)
        self.assertIn("other_1", sql_text)
        self.assertIn("other_2", sql_text)
        self.assertIn("other_3plus", sql_text)
        self.assertIn("MAX((SELECT ep.value.int_value", sql_text)
        self.assertIn("COUNT(*) AS retained_users", sql_text)
        self.assertIn("SAFE_DIVIDE(COUNT(*), st.total_users)", sql_text)
        self.assertIn("event_name = 'lib_fullscreen_ad_killed'", sql_text)
        self.assertIn("WHERE ep.key = 'activity_id') = 0", sql_text)


if __name__ == "__main__":
    unittest.main()
