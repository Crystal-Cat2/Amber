import os
import shutil
import sys
import unittest
from unittest import mock


SERVER_DIR = os.path.dirname(__file__)
if SERVER_DIR not in sys.path:
    sys.path.insert(0, SERVER_DIR)

import processor  # noqa: E402


def make_case_dir(name: str) -> str:
    root = os.path.join(os.path.dirname(os.path.dirname(SERVER_DIR)), ".tmp", "page_comment_tests")
    os.makedirs(root, exist_ok=True)
    path = os.path.join(root, name)
    if os.path.exists(path):
        shutil.rmtree(path, ignore_errors=True)
    os.makedirs(path, exist_ok=True)
    return path


class ProcessorRefactorTests(unittest.TestCase):
    def test_build_amber_prompt_cli_includes_direct_edit_contract(self):
        case_dir = make_case_dir("prompt_cli_case")

        script_path = os.path.join(case_dir, "build_dashboard.py")
        csv_path = os.path.join(case_dir, "demo.csv")
        try:
            with open(script_path, "w", encoding="utf-8") as f:
                f.write("print('hello')\n")
            with open(csv_path, "w", encoding="utf-8") as f:
                f.write("event_date,value\n2026-04-01,10\n")

            source_info = {
                "script_path": script_path,
                "csv_files": [csv_path],
                "data_dir": case_dir,
            }
            data = {
                "selected_text": "图表附近文字",
                "comment": "把这个图改成折线图",
                "chart_info": {
                    "canvas_id": "compositionChart",
                    "element_id": "compositionChart",
                    "library": "chartjs",
                    "title": "用户构成",
                    "chart_type": "bar",
                    "config_summary": "A 组, B 组",
                    "series_summary": "A 组; B 组",
                },
            }

            prompt = processor._build_amber_prompt_cli(data, source_info)
            self.assertIn("你需要直接修改工作区中的源文件来完成它", prompt)
            self.assertIn(script_path, prompt)
            self.assertIn(csv_path, prompt)
            self.assertIn("不要直接编辑生成后的 HTML", prompt)
            self.assertIn("完成后请只用中文简要说明", prompt)
            self.assertNotIn('"edits"', prompt)
            self.assertNotIn("old 字段", prompt)
        finally:
            for path in (script_path, csv_path):
                if os.path.exists(path):
                    os.remove(path)

    def test_get_modifiable_files_includes_script_and_all_csvs(self):
        temp_dir = make_case_dir("modifiable_files_case")
        script_path = os.path.join(temp_dir, "build_dashboard.py")
        data_dir = os.path.join(temp_dir, "data")
        os.makedirs(data_dir, exist_ok=True)
        csv_a = os.path.join(data_dir, "a.csv")
        csv_b = os.path.join(data_dir, "b.csv")
        txt_path = os.path.join(data_dir, "notes.txt")
        for path in (script_path, csv_a, csv_b, txt_path):
            with open(path, "w", encoding="utf-8") as f:
                f.write("demo\n")

        result = processor._get_modifiable_files(
            {
                "script_path": script_path,
                "csv_files": [csv_a],
                "data_dir": data_dir,
            }
        )

        self.assertEqual(result[0], script_path)
        self.assertIn(csv_a, result)
        self.assertIn(csv_b, result)
        self.assertNotIn(txt_path, result)

    def test_detect_changes_returns_only_mutated_files(self):
        temp_dir = make_case_dir("detect_changes_case")
        file_a = os.path.join(temp_dir, "a.py")
        file_b = os.path.join(temp_dir, "b.csv")
        with open(file_a, "w", encoding="utf-8") as f:
            f.write("alpha\n")
        with open(file_b, "w", encoding="utf-8") as f:
            f.write("beta\n")

        before = processor._snapshot_files([file_a, file_b])
        with open(file_b, "w", encoding="utf-8") as f:
            f.write("beta-changed\n")
        after = processor._snapshot_files([file_a, file_b])

        changed = processor._detect_changes(before, after)
        self.assertEqual(changed, [file_b])

    def test_resolve_session_prefers_requested_session_when_page_key_matches(self):
        requested = {"id": "s-1", "page_key": "demo::page.html", "cli_session_id": "cli-1"}
        with mock.patch.object(processor.store, "get_session", return_value=requested) as get_session:
            with mock.patch.object(processor.store, "get_active_session") as get_active:
                result = processor._resolve_session("demo::page.html", "file:///demo.html", "s-1")

        self.assertEqual(result, requested)
        get_session.assert_called_once_with("s-1")
        get_active.assert_not_called()

    def test_process_comment_amber_dashboard_uses_cli_direct_edit_and_reload(self):
        async def run_case():
            statuses = []

            async def status_callback(status, message):
                statuses.append((status, message))

            source_info = {
                "script_path": r"D:\Work\Amber\ad_kill\scripts\run_user_distribution_dashboard.py",
                "csv_files": [r"D:\Work\Amber\ad_kill\data\ad_kill_dau_user_composition.csv"],
                "data_dir": r"D:\Work\Amber\ad_kill\data",
                "output_html": r"D:\Work\Amber\ad_kill\outputs\ad_kill_user_distribution.html",
                "run_command": ["python", r"D:\Work\Amber\ad_kill\scripts\run_user_distribution_dashboard.py", "--skip-query"],
                "project_root": r"D:\Work\Amber\ad_kill",
            }
            session = {"id": "session-1", "page_key": "ad_kill/scripts/run_user_distribution_dashboard.py::ad_kill_user_distribution.html", "cli_session_id": None, "title": None}
            user_msg = {"id": "user-msg-1"}

            with mock.patch.object(processor, "resolve_source", return_value=source_info):
                with mock.patch.object(processor.store, "get_session", return_value=None):
                    with mock.patch.object(processor.store, "get_active_session", return_value=session):
                        with mock.patch.object(processor.store, "add_message", side_effect=[user_msg, {"id": "assistant-msg-1"}]) as add_message:
                            with mock.patch.object(processor, "_call_claude_cli", return_value=("已把图表改成折线图。", "cli-session-1")) as call_cli:
                                with mock.patch.object(processor, "_snapshot_files", side_effect=[
                                    {"a.py": "old"},
                                    {"a.py": "new"},
                                ]):
                                    with mock.patch.object(processor, "_get_modifiable_files", return_value=["a.py"]):
                                        with mock.patch.object(processor, "_backup_files", return_value={"a.py": "a.py.pc_backup"}):
                                            with mock.patch.object(processor, "_regenerate", return_value=True) as regenerate:
                                                with mock.patch.object(processor, "_cleanup_backups") as cleanup:
                                                    result = await processor.process_comment(
                                                        {
                                                            "selected_text": "图表附近文字",
                                                            "comment": "改成折线图",
                                                            "page_url": "file:///D:/Work/Amber/ad_kill/outputs/ad_kill_user_distribution.html",
                                                            "page_title": "Ad Kill 用户分布",
                                                            "page_meta": {"source_script": "ad_kill/scripts/run_user_distribution_dashboard.py"},
                                                        },
                                                        status_callback,
                                                    )

            self.assertEqual(result["action"], "reload")
            self.assertEqual(result["session_id"], "session-1")
            self.assertEqual(result["page_key"], session["page_key"])
            self.assertIn(("editing", "正在让模型直接修改源文件..."), statuses)
            call_cli.assert_called_once()
            regenerate.assert_called_once()
            cleanup.assert_called_once()
            assistant_call = add_message.call_args_list[-1]
            self.assertIn('["a.py"]', assistant_call.kwargs["edits_json"])
            self.assertEqual(assistant_call.kwargs["edit_success"], 1)

        import asyncio

        asyncio.run(run_case())


if __name__ == "__main__":
    unittest.main()
