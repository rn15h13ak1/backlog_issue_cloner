"""
Backlog 課題クローン 対話メニュー ユニットテスト
=================================================
入力値の検証・引数の組み立て・前回値の記憶を、本体を実行せずに検証する。
対話ループ本体（main）は副作用が大きいため対象外。
"""

import json
import pathlib
import tempfile
import unittest
from datetime import date
from io import StringIO
from unittest.mock import MagicMock, patch

import menu


# ===========================================================================
# 課題キーの検証
# ===========================================================================


class TestIsIssueKey(unittest.TestCase):
    def test_valid(self):
        for key in ("PROJ-1", "PROJ-123", "A1-9", "MY_PROJ-42"):
            self.assertTrue(menu.is_issue_key(key), key)

    def test_project_key_may_start_with_digit_or_underscore(self):
        """Backlog のプロジェクトキーは数字やアンダースコアで始められる。"""
        for key in ("10_AAA-20", "1-1", "_X-1", "2024_PROJ-100"):
            self.assertTrue(menu.is_issue_key(key), key)

    def test_invalid(self):
        for key in ("PROJ", "123", "-1", "PROJ-", "PROJ 1", "", "PROJ-1.2",
                    "PROJ-1-2", "PROJ-abc", "日本語-1"):
            self.assertFalse(menu.is_issue_key(key), key)


# ===========================================================================
# 日付
# ===========================================================================


class TestDatePresets(unittest.TestCase):
    def test_today_and_tomorrow(self):
        presets = menu.date_presets(date(2026, 9, 9))  # 水曜
        self.assertEqual(presets[0][1], "20260909")
        self.assertEqual(presets[1][1], "20260910")

    def test_next_monday_from_midweek(self):
        presets = menu.date_presets(date(2026, 9, 9))  # 水曜 → 翌週月曜は 9/14
        self.assertEqual(presets[2][1], "20260914")

    def test_next_monday_from_monday_is_next_week(self):
        """月曜に実行したら「今日」ではなく翌週の月曜を指す。"""
        presets = menu.date_presets(date(2026, 9, 14))  # 月曜
        self.assertEqual(presets[2][1], "20260921")

    def test_next_monday_from_sunday(self):
        presets = menu.date_presets(date(2026, 9, 13))  # 日曜 → 翌日が月曜
        self.assertEqual(presets[2][1], "20260914")

    def test_valid_date_helper(self):
        self.assertTrue(menu._valid_date("20260401"))
        self.assertFalse(menu._valid_date("2026-04-01"))
        self.assertFalse(menu._valid_date("なにか"))


# ===========================================================================
# 引数の組み立て
# ===========================================================================


class TestBuildArgs(unittest.TestCase):
    def test_simple_mode(self):
        args = menu.build_args(menu.MODE_SIMPLE, "PROJ-1")
        self.assertEqual(args, ["--source-issue-key", "PROJ-1", "--no-summary-template"])

    def test_simple_mode_execute(self):
        args = menu.build_args(menu.MODE_SIMPLE, "PROJ-1", execute=True)
        self.assertIn("--execute", args)

    def test_periodic_mode_with_date(self):
        args = menu.build_args(
            menu.MODE_PERIODIC, "PROJ-1",
            template="【定期】{YYYYMMDD}", date_str="20260909",
        )
        self.assertEqual(args, [
            "--source-issue-key", "PROJ-1",
            "--summary-template", "【定期】{YYYYMMDD}",
            "--date", "20260909",
        ])

    def test_periodic_mode_without_date(self):
        """日付を含まないテンプレートでは --date を渡さない。"""
        args = menu.build_args(menu.MODE_PERIODIC, "PROJ-1", template="固定の件名")
        self.assertNotIn("--date", args)

    def test_update_mode(self):
        args = menu.build_args(menu.MODE_UPDATE, "PROJ-1", target_key="DEST-5")
        self.assertEqual(args, [
            "--source-issue-key", "PROJ-1", "--target-issue-key", "DEST-5",
        ])

    def test_update_mode_does_not_pass_template(self):
        args = menu.build_args(
            menu.MODE_UPDATE, "PROJ-1", target_key="DEST-5", template="無視される"
        )
        self.assertNotIn("--summary-template", args)

    def test_config_path_is_passed_first(self):
        args = menu.build_args(menu.MODE_SIMPLE, "PROJ-1", config_path="my.yaml")
        self.assertEqual(args[:2], ["--config", "my.yaml"])

    def test_yes_is_never_passed(self):
        """確認は本体側で行うため、メニューからは --yes を渡さない。"""
        for mode in (menu.MODE_SIMPLE, menu.MODE_PERIODIC, menu.MODE_UPDATE):
            args = menu.build_args(mode, "PROJ-1", template="t", target_key="D-1",
                                   execute=True)
            self.assertNotIn("--yes", args)

    def test_built_args_are_accepted_by_the_tool(self):
        """組み立てた引数が本体のパーサで解釈できることを確認する。"""
        import backlog_issue_cloner as cloner
        for mode, extra in (
            (menu.MODE_SIMPLE, {}),
            (menu.MODE_PERIODIC, {"template": "【定期】{YYYYMMDD}", "date_str": "20260909"}),
            (menu.MODE_UPDATE, {"target_key": "DEST-5"}),
        ):
            args = menu.build_args(mode, "PROJ-1", execute=True, **extra)
            parsed = cloner.build_parser().parse_args(args)
            self.assertEqual(parsed.source_issue_key, "PROJ-1")
            self.assertTrue(parsed.execute)


# ===========================================================================
# 入力
# ===========================================================================


class TestInputText(unittest.TestCase):
    def test_returns_entered_value(self):
        with patch("builtins.input", return_value=" PROJ-1 "):
            self.assertEqual(menu.input_text("キー"), "PROJ-1")

    def test_empty_returns_default(self):
        with patch("builtins.input", return_value=""):
            self.assertEqual(menu.input_text("キー", default="PROJ-9"), "PROJ-9")

    def test_empty_without_default_returns_none(self):
        with patch("builtins.input", return_value=""):
            self.assertIsNone(menu.input_text("キー"))

    def test_reprompts_until_valid(self):
        with patch("sys.stdout", new_callable=StringIO), \
             patch("builtins.input", side_effect=["だめ", "PROJ-1"]):
            result = menu.input_text("キー", validate=menu.is_issue_key, hint="形式")
        self.assertEqual(result, "PROJ-1")


class TestPrintMenu(unittest.TestCase):
    def _menu(self, answers, **kw):
        out = StringIO()
        with patch("sys.stdout", out), patch("builtins.input", side_effect=answers):
            choice = menu.print_menu("タイトル", ["A", "B", "C"], **kw)
        return choice, out.getvalue()

    def test_returns_selected_number(self):
        self.assertEqual(self._menu(["2"])[0], 2)

    def test_zero_returns_back(self):
        self.assertEqual(self._menu(["0"])[0], 0)

    def test_empty_uses_default(self):
        self.assertEqual(self._menu([""], default=3)[0], 3)

    def test_empty_without_default_reprompts(self):
        self.assertEqual(self._menu(["", "1"])[0], 1)

    def test_out_of_range_reprompts(self):
        choice, text = self._menu(["9", "2"])
        self.assertEqual(choice, 2)
        self.assertIn("無効な入力", text)

    def test_non_numeric_reprompts(self):
        self.assertEqual(self._menu(["あ", "1"])[0], 1)

    def test_default_mark_is_configurable(self):
        _, text = self._menu(["1"], default=2, default_mark="前回")
        self.assertIn("←前回", text)
        _, text = self._menu(["1"], default=2)
        self.assertIn("←既定", text)

    def test_back_label_is_shown(self):
        _, text = self._menu(["1"], back_label="終了")
        self.assertIn("0. 終了", text)


class TestChooseDate(unittest.TestCase):
    def test_preset_returns_date(self):
        with patch("sys.stdout", new_callable=StringIO), \
             patch.object(menu, "print_menu", return_value=1), \
             patch.object(menu, "date_presets",
                          return_value=[("今日", "20260909"), ("明日", "20260910")]):
            self.assertEqual(menu.choose_date(), "20260909")

    def test_back_returns_none(self):
        with patch.object(menu, "print_menu", return_value=0):
            self.assertIsNone(menu.choose_date())

    def test_manual_entry(self):
        """プリセットの次の番号が手動入力。"""
        presets = [("今日", "20260909"), ("明日", "20260910"), ("来週", "20260914")]
        with patch("sys.stdout", new_callable=StringIO), \
             patch.object(menu, "date_presets", return_value=presets), \
             patch.object(menu, "print_menu", return_value=4), \
             patch("builtins.input", return_value="20260401"):
            self.assertEqual(menu.choose_date(), "20260401")

    def test_manual_entry_rejects_bad_format(self):
        presets = [("今日", "20260909")]
        with patch("sys.stdout", new_callable=StringIO), \
             patch.object(menu, "date_presets", return_value=presets), \
             patch.object(menu, "print_menu", return_value=2), \
             patch("builtins.input", side_effect=["2026-04-01", "20260401"]):
            self.assertEqual(menu.choose_date(), "20260401")


# ===========================================================================
# 前回値の記憶
# ===========================================================================


class TestHistory(unittest.TestCase):
    def setUp(self):
        self._dir = tempfile.TemporaryDirectory()
        self.addCleanup(self._dir.cleanup)
        self.path = pathlib.Path(self._dir.name) / "history.json"
        patcher = patch.object(menu, "HISTORY_PATH", self.path)
        patcher.start()
        self.addCleanup(patcher.stop)

    def test_missing_file_returns_empty(self):
        self.assertEqual(menu.load_history(), {})

    def test_round_trip(self):
        menu.save_history({"source_issue_key": "PROJ-1", "mode": "simple"})
        self.assertEqual(menu.load_history()["source_issue_key"], "PROJ-1")

    def test_broken_file_returns_empty(self):
        self.path.write_text("{ではないJSON", encoding="utf-8")
        self.assertEqual(menu.load_history(), {})

    def test_non_dict_returns_empty(self):
        self.path.write_text(json.dumps([1, 2]), encoding="utf-8")
        self.assertEqual(menu.load_history(), {})

    def test_save_failure_is_ignored(self):
        with patch.object(menu, "HISTORY_PATH", pathlib.Path("/存在しない/h.json")):
            menu.save_history({"a": 1})  # 例外が出なければ OK


# ===========================================================================
# 接続先の表示
# ===========================================================================


class TestDescribeConnection(unittest.TestCase):
    def setUp(self):
        self._dir = tempfile.TemporaryDirectory()
        self.addCleanup(self._dir.cleanup)
        self.dir = pathlib.Path(self._dir.name)

    def _write(self, text):
        path = self.dir / "c.yaml"
        path.write_text(text, encoding="utf-8")
        return str(path)

    def test_shows_host(self):
        path = self._write('backlog:\n  space_host: "myteam.backlog.com"\n')
        self.assertEqual(menu.describe_connection(path), "myteam.backlog.com")

    def test_missing_file(self):
        result = menu.describe_connection(str(self.dir / "無い.yaml"))
        self.assertIn("設定ファイルが見つかりません", result)

    def test_empty_file(self):
        self.assertIn("空", menu.describe_connection(self._write("")))

    def test_placeholder_host(self):
        path = self._write('backlog:\n  space_host: "yourcompany.backlog.com"\n')
        self.assertIn("未設定", menu.describe_connection(path))

    def test_missing_backlog_section(self):
        self.assertIn("未設定", menu.describe_connection(self._write("clone: {}\n")))


# ===========================================================================
# モードごとの対話
# ===========================================================================


class TestCollectInputs(unittest.TestCase):
    def test_simple_mode_asks_only_source(self):
        with patch.object(menu, "input_text", return_value="PROJ-1") as ask:
            result = menu.collect_inputs(menu.MODE_SIMPLE, {})
        self.assertEqual(result, {"source_key": "PROJ-1"})
        self.assertEqual(ask.call_count, 1)

    def test_cancelling_source_returns_none(self):
        with patch.object(menu, "input_text", return_value=None):
            self.assertIsNone(menu.collect_inputs(menu.MODE_SIMPLE, {}))

    def test_periodic_mode_asks_date_when_template_has_placeholder(self):
        with patch.object(menu, "input_text", side_effect=["PROJ-1", "【定期】{YYYYMMDD}"]), \
             patch.object(menu, "choose_date", return_value="20260909") as pick:
            result = menu.collect_inputs(menu.MODE_PERIODIC, {})
        pick.assert_called_once()
        self.assertEqual(result["template"], "【定期】{YYYYMMDD}")
        self.assertEqual(result["date_str"], "20260909")

    def test_periodic_mode_skips_date_without_placeholder(self):
        with patch.object(menu, "input_text", side_effect=["PROJ-1", "固定の件名"]), \
             patch.object(menu, "choose_date") as pick:
            result = menu.collect_inputs(menu.MODE_PERIODIC, {})
        pick.assert_not_called()
        self.assertNotIn("date_str", result)

    def test_update_mode_asks_target(self):
        with patch.object(menu, "input_text", side_effect=["PROJ-1", "DEST-5"]):
            result = menu.collect_inputs(menu.MODE_UPDATE, {})
        self.assertEqual(result["target_key"], "DEST-5")

    def test_update_mode_rejects_same_key(self):
        with patch("sys.stdout", new_callable=StringIO), \
             patch.object(menu, "input_text", side_effect=["PROJ-1", "proj-1"]):
            self.assertIsNone(menu.collect_inputs(menu.MODE_UPDATE, {}))

    def test_history_is_offered_as_default(self):
        history = {"source_issue_key": "PROJ-7"}
        with patch.object(menu, "input_text", return_value="PROJ-7") as ask:
            menu.collect_inputs(menu.MODE_SIMPLE, history)
        self.assertEqual(ask.call_args[1]["default"], "PROJ-7")


class TestRunMode(unittest.TestCase):
    def setUp(self):
        patcher = patch.object(menu, "save_history")
        self.save = patcher.start()
        self.addCleanup(patcher.stop)

    def _run(self, mode, inputs, choice):
        with patch("sys.stdout", new_callable=StringIO), \
             patch.object(menu, "collect_inputs", return_value=inputs), \
             patch.object(menu, "print_menu", return_value=choice), \
             patch.object(menu, "run_cloner", return_value=0) as run:
            rc = menu.run_mode(mode, "ラベル", "", {})
        return rc, run

    def test_dry_run_does_not_pass_execute(self):
        rc, run = self._run(menu.MODE_SIMPLE, {"source_key": "PROJ-1"}, 1)
        self.assertEqual(rc, 0)
        self.assertNotIn("--execute", run.call_args[0][0])

    def test_execute_passes_execute(self):
        _, run = self._run(menu.MODE_SIMPLE, {"source_key": "PROJ-1"}, 2)
        self.assertIn("--execute", run.call_args[0][0])

    def test_cancelling_mode_menu_returns_none(self):
        rc, run = self._run(menu.MODE_SIMPLE, {"source_key": "PROJ-1"}, 0)
        self.assertIsNone(rc)
        run.assert_not_called()

    def test_cancelling_inputs_returns_none(self):
        with patch("sys.stdout", new_callable=StringIO), \
             patch.object(menu, "collect_inputs", return_value=None), \
             patch.object(menu, "run_cloner") as run:
            self.assertIsNone(menu.run_mode(menu.MODE_SIMPLE, "ラベル", "", {}))
        run.assert_not_called()

    def test_history_saved_before_running(self):
        history = {}
        with patch("sys.stdout", new_callable=StringIO), \
             patch.object(menu, "collect_inputs",
                          return_value={"source_key": "PROJ-1", "target_key": "DEST-5"}), \
             patch.object(menu, "print_menu", return_value=2), \
             patch.object(menu, "run_cloner", return_value=0):
            menu.run_mode(menu.MODE_UPDATE, "ラベル", "", history)
        self.save.assert_called_once()
        self.assertEqual(history["source_issue_key"], "PROJ-1")
        self.assertEqual(history["target_issue_key"], "DEST-5")
        self.assertEqual(history["mode"], menu.MODE_UPDATE)


class TestBuildParser(unittest.TestCase):
    def test_default_config_points_at_tool_dir(self):
        args = menu.build_parser().parse_args([])
        self.assertTrue(args.config.endswith("config.yaml"))
        self.assertTrue(args.config.startswith(str(menu.TOOL_DIR)))

    def test_config_override(self):
        self.assertEqual(
            menu.build_parser().parse_args(["--config", "other.yaml"]).config,
            "other.yaml",
        )


class TestRunCloner(unittest.TestCase):
    def test_missing_script_returns_127(self):
        with patch("sys.stdout", new_callable=StringIO), \
             patch.object(menu, "SCRIPT", pathlib.Path("/存在しない/x.py")):
            self.assertEqual(menu.run_cloner([]), 127)

    def test_returns_child_exit_code(self):
        completed = MagicMock(returncode=11)
        with patch("sys.stdout", new_callable=StringIO), \
             patch("menu.subprocess.run", return_value=completed) as run:
            self.assertEqual(menu.run_cloner(["--execute"]), 11)
        self.assertEqual(run.call_args[0][0][-1], "--execute")

    def test_keyboard_interrupt_returns_130(self):
        with patch("sys.stdout", new_callable=StringIO), \
             patch("menu.subprocess.run", side_effect=KeyboardInterrupt):
            self.assertEqual(menu.run_cloner([]), menu.EXIT_INTERRUPTED)


if __name__ == "__main__":
    unittest.main(verbosity=2)
