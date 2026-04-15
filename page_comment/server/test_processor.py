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

    def test_build_generic_prompt_prefers_feishu_target_context(self):
        prompt = processor._build_generic_prompt_cli(
            {
                "selected_text": "",
                "comment": "把这个对象改一下",
                "page_title": "飞书文档",
                "page_url": "https://xwbo3y4nxr.feishu.cn/docx/AIzedZyZZoX5vtxM3UKcHKsQnyh",
                "page_context": {
                    "mode": "visible_main",
                    "content": "这是整页摘要，不该作为主上下文",
                    "truncated": False,
                },
                "target_context": {
                    "page_type": "feishu_doc",
                    "target_type": "table",
                    "target_text": "命中的表格摘要",
                    "surrounding_blocks": ["结果", "AA 实验确认"],
                    "object_meta": {
                        "title": "广告格式表",
                        "tokens": [{"kind": "sheet", "token": "shtcn123"}],
                    },
                },
                "visual_context": {
                    "format": "png",
                    "width": 420,
                    "height": 220,
                    "file_path": r"D:\Work\Amber\page_comment\.tmp\feishu_target.png",
                },
            }
        )

        self.assertIn("飞书页面命中对象", prompt)
        self.assertIn("target_type: table", prompt)
        self.assertIn("命中的表格摘要", prompt)
        self.assertIn("广告格式表", prompt)
        self.assertIn("shtcn123", prompt)
        self.assertIn(r"D:\Work\Amber\page_comment\.tmp\feishu_target.png", prompt)
        self.assertNotIn("这是整页摘要，不该作为主上下文", prompt)

    def test_resolve_session_prefers_requested_session_when_page_key_matches(self):
        requested = {"id": "s-1", "page_key": "demo::page.html", "cli_session_id": "cli-1"}
        with mock.patch.object(processor.store, "get_session", return_value=requested) as get_session:
            with mock.patch.object(processor.store, "get_active_session") as get_active:
                result = processor._resolve_session("demo::page.html", "file:///demo.html", "s-1")

        self.assertEqual(result, requested)
        get_session.assert_called_once_with("s-1")
        get_active.assert_not_called()

    def test_resolve_page_key_prefers_meta_page_key(self):
        page_key = processor._resolve_page_key(
            "https://10.0.0.252:11005/page/123?from=share#top",
            {"page_key": "ad_kill/scripts/run_user_distribution_dashboard.py::ad_kill_user_distribution.html"},
            None,
        )
        self.assertEqual(page_key, "ad_kill/scripts/run_user_distribution_dashboard.py::ad_kill_user_distribution.html")

    def test_build_resume_command_uses_provider_specific_syntax(self):
        self.assertEqual(
            processor._build_resume_command("claude", "session-1"),
            [r"C:\Users\ASUS\.local\bin\claude.exe", "--resume", "session-1"],
        )
        self.assertEqual(
            processor._build_resume_command("codex", "session-2"),
            list(processor.CODEX_CLI) + ["exec", "--json", "--full-auto", "--skip-git-repo-check", "resume", "session-2"],
        )

    def test_handle_non_amber_switches_existing_session_to_requested_provider(self):
        async def run_case():
            async def status_callback(_status, _message):
                return None

            session = {
                "id": "session-generic-2",
                "page_key": "https://example.com/report",
                "page_url": "https://example.com/report",
                "cli_session_id": "claude-session-1",
                "title": "旧会话",
                "session_type": "normal",
                "model_provider": "claude",
            }
            user_msg = {"id": "user-msg-switch-provider"}

            with mock.patch.object(processor, "resolve_source", return_value=None):
                with mock.patch.object(processor.store, "get_session", return_value=session):
                    with mock.patch.object(processor.store, "add_message", side_effect=[user_msg, {"id": "assistant-msg-1"}]):
                        with mock.patch.object(processor.store, "update_session") as update_session:
                            with mock.patch.object(processor, "_call_cli", return_value=("已切到 Codex。", "codex-session-1")) as call_cli:
                                result = await processor.process_comment(
                                    {
                                        "selected_text": "",
                                        "comment": "继续处理",
                                        "page_url": "https://example.com/report?from=share#intro",
                                        "page_title": "示例报告",
                                        "page_context": {
                                            "mode": "visible_main",
                                            "content": "这是页面主体内容",
                                            "truncated": False,
                                        },
                                        "page_meta": {},
                                        "session_id": "session-generic-2",
                                        "model_provider": "codex",
                                    },
                                    status_callback,
                                )

            self.assertEqual(result["session"]["model_provider"], "codex")
            self.assertEqual(result["session"]["cli_session_id"], "codex-session-1")
            self.assertEqual(call_cli.call_args.args[1], None)
            self.assertEqual(call_cli.call_args.kwargs["model_provider"], "codex")
            update_session.assert_any_call("session-generic-2", model_provider="codex", cli_session_id=None)
            update_session.assert_any_call("session-generic-2", cli_session_id="codex-session-1")

        import asyncio

        asyncio.run(run_case())

    def test_process_comment_returns_friendly_message_for_stale_resume_error(self):
        async def run_case():
            async def status_callback(_status, _message):
                return None

            source_info = {
                "script_path": r"D:\Work\Amber\ad_kill\scripts\run_user_distribution_dashboard.py",
                "csv_files": [r"D:\Work\Amber\ad_kill\data\ad_kill_dau_user_composition.csv"],
                "data_dir": r"D:\Work\Amber\ad_kill\data",
                "output_html": r"D:\Work\Amber\ad_kill\outputs\ad_kill_user_distribution.html",
                "project_root": r"D:\Work\Amber\ad_kill",
            }
            session = {
                "id": "session-stale-1",
                "page_key": "ad_kill/scripts/run_user_distribution_dashboard.py::ad_kill_user_distribution.html",
                "cli_session_id": "stale-session-1",
                "title": None,
                "model_provider": "claude",
            }
            user_msg = {"id": "user-msg-stale-1"}

            with mock.patch.object(processor, "resolve_source", return_value=source_info):
                with mock.patch.object(processor.store, "get_session", return_value=None):
                    with mock.patch.object(processor.store, "get_active_session", return_value=session):
                        with mock.patch.object(processor.store, "add_message", side_effect=[user_msg, {"id": "assistant-msg-1"}]):
                            with mock.patch.object(processor, "_get_modifiable_files", return_value=["a.py"]):
                                with mock.patch.object(processor, "_snapshot_files", return_value={"a.py": "old"}):
                                    with mock.patch.object(processor, "_backup_files", return_value={"a.py": "a.py.pc_backup"}):
                                        with mock.patch.object(processor, "_cleanup_backups"):
                                            with mock.patch.object(
                                                processor,
                                                "_call_cli",
                                                side_effect=RuntimeError(
                                                    "claude CLI 返回错误: No conversation found with session ID: stale-session-1"
                                                ),
                                            ):
                                                result = await processor.process_comment(
                                                    {
                                                        "selected_text": "图表附近文字",
                                                        "comment": "改一下",
                                                        "page_url": "file:///D:/Work/Amber/ad_kill/outputs/ad_kill_user_distribution.html",
                                                        "page_title": "Ad Kill 用户分布",
                                                        "page_meta": {"source_script": "ad_kill/scripts/run_user_distribution_dashboard.py"},
                                                    },
                                                    status_callback,
                                                )

            self.assertIn("无法续用会话", result["response"])
            self.assertEqual(result["action"], "none")
            self.assertEqual(result["cli_session_id"], "stale-session-1")

        import asyncio

        asyncio.run(run_case())

    def test_process_comment_skips_creator_session_auto_link_for_codex(self):
        async def run_case():
            async def status_callback(_status, _message):
                return None

            source_info = {
                "script_path": r"D:\Work\Amber\ad_kill\scripts\run_user_distribution_dashboard.py",
                "csv_files": [r"D:\Work\Amber\ad_kill\data\ad_kill_dau_user_composition.csv"],
                "data_dir": r"D:\Work\Amber\ad_kill\data",
                "output_html": r"D:\Work\Amber\ad_kill\outputs\ad_kill_user_distribution.html",
                "project_root": r"D:\Work\Amber\ad_kill",
            }
            session = {
                "id": "session-codex-creator-1",
                "page_key": "ad_kill/scripts/run_user_distribution_dashboard.py::ad_kill_user_distribution.html",
                "cli_session_id": None,
                "title": None,
                "session_type": "normal",
                "model_provider": "codex",
            }
            user_msg = {"id": "user-msg-codex-creator-1"}

            with mock.patch.object(processor, "resolve_source", return_value=source_info):
                with mock.patch.object(processor.store, "get_session", return_value=session):
                    with mock.patch.object(processor.store, "add_message", side_effect=[user_msg, {"id": "assistant-msg-1"}]):
                        with mock.patch.object(processor.store, "update_session") as update_session:
                            with mock.patch.object(processor, "_get_modifiable_files", return_value=["a.py"]):
                                with mock.patch.object(processor, "_snapshot_files", side_effect=[{"a.py": "old"}, {"a.py": "old"}]):
                                    with mock.patch.object(processor, "_backup_files", return_value={"a.py": "a.py.pc_backup"}):
                                        with mock.patch.object(processor, "_cleanup_backups"):
                                            with mock.patch.object(processor, "_call_cli", return_value=("已处理", "codex-session-2")) as call_cli:
                                                result = await processor.process_comment(
                                                    {
                                                        "selected_text": "图表附近文字",
                                                        "comment": "改一下",
                                                        "page_url": "file:///D:/Work/Amber/ad_kill/outputs/ad_kill_user_distribution.html",
                                                        "page_title": "Ad Kill 用户分布",
                                                        "page_meta": {
                                                            "source_script": "ad_kill/scripts/run_user_distribution_dashboard.py",
                                                            "creator_session": "claude-creator-session",
                                                        },
                                                        "session_id": "session-codex-creator-1",
                                                        "model_provider": "codex",
                                                    },
                                                    status_callback,
                                                )

            self.assertEqual(result["session"]["model_provider"], "codex")
            self.assertEqual(call_cli.call_args.args[1], None)
            update_session.assert_any_call("session-codex-creator-1", cli_session_id="codex-session-2")
            self.assertFalse(
                any(
                    kwargs.get("cli_session_id") == "claude-creator-session"
                    for _args, kwargs in update_session.call_args_list
                )
            )

        import asyncio

        asyncio.run(run_case())

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
                            with mock.patch.object(processor, "_call_cli", return_value=("已把图表改成折线图。", "cli-session-1")) as call_cli:
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
            self.assertIn(("editing", "AI 正在思考..."), statuses)
            call_cli.assert_called_once()
            regenerate.assert_called_once()
            cleanup.assert_called_once()
            assistant_call = add_message.call_args_list[-1]
            self.assertIn('["a.py"]', assistant_call.kwargs["edits_json"])
            self.assertEqual(assistant_call.kwargs["edit_success"], 1)

        import asyncio

        asyncio.run(run_case())

    def test_process_comment_generic_creates_session_and_returns_session_payload(self):
        async def run_case():
            statuses = []

            async def status_callback(status, message):
                statuses.append((status, message))

            created_session = {
                "id": "session-generic-1",
                "page_key": "https://example.com/report",
                "page_url": "https://example.com/report?from=share",
                "cli_session_id": None,
                "title": None,
                "session_type": "normal",
                "model_provider": None,
            }
            user_msg = {"id": "user-msg-1"}

            with mock.patch.object(processor, "resolve_source", return_value=None):
                with mock.patch.object(processor.store, "get_session", return_value=None):
                    with mock.patch.object(processor.store, "get_active_session", return_value=None):
                        with mock.patch.object(processor.store, "create_session", return_value=created_session) as create_session:
                            with mock.patch.object(processor.store, "add_message", side_effect=[user_msg, {"id": "assistant-msg-1"}]) as add_message:
                                with mock.patch.object(processor.store, "update_session") as update_session:
                                    with mock.patch.object(processor, "_call_cli", return_value=("已根据页面内容回答。", "cli-generic-1")) as call_cli:
                                        result = await processor.process_comment(
                                            {
                                                "selected_text": "",
                                                "comment": "帮我总结这个页面",
                                                "page_url": "https://example.com/report?from=share#intro",
                                                "page_title": "示例报告",
                                                "page_context": {
                                                    "mode": "visible_main",
                                                    "content": "这是页面主体内容",
                                                    "truncated": False,
                                                },
                                                "page_meta": {},
                                                "model_provider": "codex",
                                            },
                                            status_callback,
                                        )

            self.assertEqual(result["session_id"], "session-generic-1")
            self.assertEqual(result["cli_session_id"], "cli-generic-1")
            self.assertEqual(result["session"]["id"], "session-generic-1")
            self.assertEqual(result["session"]["cli_session_id"], "cli-generic-1")
            self.assertEqual(result["session"]["title"], "帮我总结这个页面")
            self.assertEqual(result["session"]["model_provider"], "codex")
            create_session.assert_called_once_with("https://example.com/report", "https://example.com/report?from=share#intro", model_provider="codex")
            call_cli.assert_called_once()
            self.assertEqual(call_cli.call_args.args[1], None)
            self.assertEqual(call_cli.call_args.kwargs["model_provider"], "codex")
            update_session.assert_any_call("session-generic-1", cli_session_id="cli-generic-1")
            update_session.assert_any_call("session-generic-1", title="帮我总结这个页面")
            self.assertEqual(add_message.call_args_list[0].kwargs["session_id"], "session-generic-1")
            self.assertEqual(add_message.call_args_list[1].kwargs["session_id"], "session-generic-1")
            self.assertIn(("processing", "正在回答问题..."), statuses)

        import asyncio

        asyncio.run(run_case())

    def test_process_comment_pagedoc_followup_resumes_requested_session(self):
        async def run_case():
            async def status_callback(_status, _message):
                return None

            requested_session = {
                "id": "session-pagedoc-1",
                "page_key": "https://xwbo3y4nxr.feishu.cn/docx/abcd",
                "page_url": "https://xwbo3y4nxr.feishu.cn/docx/abcd",
                "cli_session_id": "cli-pagedoc-1",
                "title": "先前问题",
                "session_type": "normal",
                "model_provider": "claude",
            }
            user_msg = {"id": "user-msg-2"}

            with mock.patch.object(processor, "resolve_source", return_value=None):
                with mock.patch.object(processor.store, "get_session", return_value=requested_session) as get_session:
                    with mock.patch.object(processor.store, "get_active_session") as get_active:
                        with mock.patch.object(processor.store, "add_message", side_effect=[user_msg, {"id": "assistant-msg-2"}]):
                            with mock.patch.object(processor.store, "update_session") as update_session:
                                with mock.patch.object(processor, "_call_cli", return_value=("继续回答。", "cli-pagedoc-1")) as call_cli:
                                    result = await processor.process_comment(
                                        {
                                            "selected_text": "",
                                            "comment": "继续",
                                            "page_url": "https://xwbo3y4nxr.feishu.cn/docx/abcd?from=bookmark",
                                            "page_title": "飞书文档",
                                            "page_context": {
                                                "mode": "visible_main",
                                                "content": "文档可见内容",
                                                "truncated": False,
                                            },
                                            "page_meta": {},
                                            "session_id": "session-pagedoc-1",
                                        },
                                        status_callback,
                                    )

            self.assertEqual(result["session_id"], "session-pagedoc-1")
            self.assertEqual(result["cli_session_id"], "cli-pagedoc-1")
            self.assertEqual(result["session"]["id"], "session-pagedoc-1")
            self.assertEqual(result["session"]["model_provider"], "claude")
            get_session.assert_called_once_with("session-pagedoc-1")
            get_active.assert_not_called()
            call_cli.assert_called_once()
            self.assertEqual(call_cli.call_args.args[1], "cli-pagedoc-1")
            self.assertEqual(call_cli.call_args.kwargs["model_provider"], "claude")
            update_session.assert_not_called()

        import asyncio

        asyncio.run(run_case())

    def test_call_cli_returns_immediately_after_claude_result_event(self):
        async def run_case():
            class FakeStdin:
                def __init__(self):
                    self.closed = False
                    self.payloads = []

                def write(self, payload):
                    self.payloads.append(payload)

                async def drain(self):
                    return None

                def is_closing(self):
                    return self.closed

                def close(self):
                    self.closed = True

            class FakeStdout:
                def __init__(self):
                    self.calls = 0

                async def readline(self):
                    self.calls += 1
                    if self.calls == 1:
                        return (
                            b'{"type":"result","result":"\\u5df2\\u5b8c\\u6210","is_error":false,'
                            b'"session_id":"cli-session-1","duration_ms":1234}\n'
                        )
                    await asyncio.sleep(1)
                    return b""

            class FakeStderr:
                async def read(self):
                    return b""

            class FakeProc:
                def __init__(self):
                    self.stdin = FakeStdin()
                    self.stdout = FakeStdout()
                    self.stderr = FakeStderr()
                    self.returncode = 0

                async def wait(self):
                    return 0

                def kill(self):
                    self.returncode = -9

            fake_proc = FakeProc()

            with mock.patch.object(processor.asyncio, "create_subprocess_exec", return_value=fake_proc):
                with mock.patch.object(processor, "CLI_TIMEOUT", 0.05):
                    result_text, session_id = await processor._call_cli(
                        "测试 prompt",
                        cli_session_id="cli-session-1",
                        cwd=r"D:\Work\Amber",
                        model_provider="claude",
                    )

            self.assertEqual(result_text, "已完成")
            self.assertEqual(session_id, "cli-session-1")

        import asyncio

        asyncio.run(run_case())


if __name__ == "__main__":
    unittest.main()
