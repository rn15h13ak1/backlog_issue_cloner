"""
Backlog 課題クローンツール ユニットテスト
==========================================
BacklogClient をモック化して、API 接続なしで動作を検証する。
"""

import sys
import types
import unittest
from io import StringIO
from unittest.mock import MagicMock, patch

# テスト対象モジュールのインポート
import backlog_issue_cloner as sut


# ===========================================================================
# BacklogClient ユニットテスト
# ===========================================================================


class TestFindExistingBySummary(unittest.TestCase):
    """find_existing_by_summary — 件名部分一致フィルタの動作を検証。"""

    def _make_client(self, issues):
        client = MagicMock()
        client.search_issues_by_keyword.return_value = issues
        return client

    def test_exact_match_returns_issue(self):
        issues = [{"issueKey": "PROJ-1", "summary": "【定期】20260828 タスク"}]
        client = self._make_client(issues)
        result = sut.find_existing_by_summary(client, 10, "【定期】20260828 タスク")
        self.assertEqual(result["issueKey"], "PROJ-1")

    def test_substring_match_returns_issue(self):
        """件名が部分一致（より長い文字列に含まれる）でも返す。"""
        issues = [{"issueKey": "PROJ-2", "summary": "【定期】20260828 タスク（コピー）"}]
        client = self._make_client(issues)
        result = sut.find_existing_by_summary(client, 10, "【定期】20260828 タスク")
        self.assertEqual(result["issueKey"], "PROJ-2")

    def test_no_match_returns_none(self):
        issues = [{"issueKey": "PROJ-3", "summary": "全く別の課題"}]
        client = self._make_client(issues)
        result = sut.find_existing_by_summary(client, 10, "【定期】20260828 タスク")
        self.assertIsNone(result)

    def test_empty_result_returns_none(self):
        client = self._make_client([])
        result = sut.find_existing_by_summary(client, 10, "【定期】20260828 タスク")
        self.assertIsNone(result)

    def test_first_match_is_returned(self):
        """複数ヒットのうち最初のものを返す。"""
        issues = [
            {"issueKey": "PROJ-1", "summary": "【定期】20260828 タスク"},
            {"issueKey": "PROJ-2", "summary": "【定期】20260828 タスク（別件）"},
        ]
        client = self._make_client(issues)
        result = sut.find_existing_by_summary(client, 10, "【定期】20260828 タスク")
        self.assertEqual(result["issueKey"], "PROJ-1")


# ===========================================================================
# resolve_date テスト
# ===========================================================================


class TestResolveDate(unittest.TestCase):
    def test_valid_date_passthrough(self):
        self.assertEqual(sut.resolve_date("20260401"), "20260401")

    def test_none_returns_today(self):
        with patch("backlog_issue_cloner.datetime") as mock_dt:
            mock_dt.now.return_value.strftime.return_value = "20260828"
            mock_dt.strptime.side_effect = lambda *a, **kw: __import__("datetime").datetime.strptime(*a, **kw)
            result = sut.resolve_date(None)
        self.assertEqual(result, "20260828")

    def test_invalid_date_exits(self):
        with self.assertRaises(SystemExit):
            sut.resolve_date("not-a-date")


# ===========================================================================
# resolve_issue_type_id テスト
# ===========================================================================


class TestResolveIssueTypeId(unittest.TestCase):
    TYPES = [
        {"id": 1, "name": "タスク"},
        {"id": 2, "name": "バグ"},
        {"id": 3, "name": "要望"},
    ]

    def _make_client(self):
        client = MagicMock()
        client.get_issue_types.return_value = self.TYPES
        return client

    def test_exact_name_match(self):
        client = self._make_client()
        id_, name = sut.resolve_issue_type_id(client, "PROJ", "バグ")
        self.assertEqual(id_, 2)
        self.assertEqual(name, "バグ")

    def test_fallback_to_first_when_not_found(self):
        client = self._make_client()
        with patch("sys.stderr", new_callable=StringIO):
            id_, name = sut.resolve_issue_type_id(client, "PROJ", "存在しない種別")
        self.assertEqual(id_, 1)
        self.assertEqual(name, "タスク")

    def test_none_returns_first(self):
        client = self._make_client()
        id_, name = sut.resolve_issue_type_id(client, "PROJ", None)
        self.assertEqual(id_, 1)
        self.assertEqual(name, "タスク")

    def test_empty_types_exits(self):
        client = MagicMock()
        client.get_issue_types.return_value = []
        with self.assertRaises(SystemExit):
            sut.resolve_issue_type_id(client, "PROJ", None)


# ===========================================================================
# resolve_priority_id テスト
# ===========================================================================


class TestResolvePriorityId(unittest.TestCase):
    PRIORITIES = [
        {"id": 2, "name": "高"},
        {"id": 3, "name": "中"},
        {"id": 4, "name": "低"},
    ]

    def _make_client(self, priorities=None):
        client = MagicMock()
        client.get_priorities.return_value = priorities if priorities is not None else self.PRIORITIES
        return client

    def test_exact_name_match(self):
        client = self._make_client()
        id_, name = sut.resolve_priority_id(client, "高")
        self.assertEqual(id_, 2)
        self.assertEqual(name, "高")

    def test_fallback_to_chuu_when_none(self):
        client = self._make_client()
        id_, name = sut.resolve_priority_id(client, None)
        self.assertEqual(id_, 3)
        self.assertEqual(name, "中")

    def test_fallback_to_first_when_chuu_not_found(self):
        priorities = [{"id": 2, "name": "高"}, {"id": 4, "name": "低"}]
        client = self._make_client(priorities)
        id_, name = sut.resolve_priority_id(client, None)
        self.assertEqual(id_, 2)
        self.assertEqual(name, "高")

    def test_empty_priorities_exits(self):
        client = self._make_client([])
        with self.assertRaises(SystemExit):
            sut.resolve_priority_id(client, None)


# ===========================================================================
# run() 統合テスト（BacklogClient 全体をモック）
# ===========================================================================


def _make_args(execute=False, date=None, debug=False, config="config.yaml"):
    args = MagicMock()
    args.execute = execute
    args.date = date
    args.debug = debug
    args.config = config
    return args


def _make_config(
    space_host="test.backlog.com",
    api_key="TESTKEY",
    source_issue_key="PROJ-1",
    summary_template="【定期】{YYYYMMDD} タスク",
    target_project_key=None,
    issue_type=None,
    priority=None,
):
    cfg = {
        "backlog": {
            "space_host": space_host,
            "api_key": api_key,
            "ssl_verify": True,
            "base_path": "",
        },
        "clone": {
            "source_issue_key": source_issue_key,
            "summary_template": summary_template,
        },
    }
    if target_project_key:
        cfg["clone"]["target_project_key"] = target_project_key
    if issue_type:
        cfg["clone"]["issue_type"] = issue_type
    if priority:
        cfg["clone"]["priority"] = priority
    return cfg


SOURCE_ISSUE = {
    "issueKey": "PROJ-1",
    "summary": "テンプレート課題",
    "description": "本文テキスト",
    "projectId": 10,
}

PROJECT = {"id": 10, "projectKey": "PROJ"}
ISSUE_TYPES = [{"id": 1, "name": "タスク"}]
PRIORITIES = [{"id": 2, "name": "高"}, {"id": 3, "name": "中"}]


class TestRunDryRun(unittest.TestCase):
    """ドライランモードでは API 書き込みが発生しないことを検証。"""

    def _patch_client(self, existing_issue=None, existing_desc=None):
        """BacklogClient をモックに差し替えるコンテキストマネージャを返す。"""
        mock_client = MagicMock()
        mock_client.get_issue.return_value = SOURCE_ISSUE
        mock_client.get_project.return_value = PROJECT
        mock_client.get_issue_types.return_value = ISSUE_TYPES
        mock_client.get_priorities.return_value = PRIORITIES

        if existing_issue:
            mock_client.search_issues_by_keyword.return_value = [existing_issue]
        else:
            mock_client.search_issues_by_keyword.return_value = []

        return patch("backlog_issue_cloner.BacklogClient", return_value=mock_client), mock_client

    def test_dry_run_no_existing_no_create(self):
        """ドライラン: 既存課題なし → create_issue は呼ばれない。"""
        patcher, mock_client = self._patch_client()
        with patcher, patch("sys.stdout", new_callable=StringIO):
            sut.run(_make_args(execute=False, date="20260828"), _make_config())
        mock_client.create_issue.assert_not_called()

    def test_dry_run_existing_same_desc_no_update(self):
        """ドライラン: 既存課題あり・本文同一 → update_issue は呼ばれない。"""
        existing = {
            "issueKey": "PROJ-99",
            "summary": "【定期】20260828 タスク",
            "description": "本文テキスト",
        }
        patcher, mock_client = self._patch_client(existing_issue=existing)
        with patcher, patch("sys.stdout", new_callable=StringIO):
            sut.run(_make_args(execute=False, date="20260828"), _make_config())
        mock_client.update_issue.assert_not_called()

    def test_dry_run_existing_diff_desc_no_update(self):
        """ドライラン: 既存課題あり・本文差分あり → update_issue は呼ばれない。"""
        existing = {
            "issueKey": "PROJ-99",
            "summary": "【定期】20260828 タスク",
            "description": "古い本文",
        }
        patcher, mock_client = self._patch_client(existing_issue=existing)
        with patcher, patch("sys.stdout", new_callable=StringIO):
            sut.run(_make_args(execute=False, date="20260828"), _make_config())
        mock_client.update_issue.assert_not_called()


class TestRunExecute(unittest.TestCase):
    """--execute モードでの作成・更新・スキップを検証。"""

    def _patch_client(self, existing_issue=None):
        mock_client = MagicMock()
        mock_client.get_issue.return_value = SOURCE_ISSUE
        mock_client.get_project.return_value = PROJECT
        mock_client.get_issue_types.return_value = ISSUE_TYPES
        mock_client.get_priorities.return_value = PRIORITIES
        mock_client.create_issue.return_value = {
            "issueKey": "PROJ-100", "summary": "【定期】20260828 タスク"
        }
        mock_client.update_issue.return_value = {
            "issueKey": "PROJ-99", "summary": "【定期】20260828 タスク"
        }

        if existing_issue:
            mock_client.search_issues_by_keyword.return_value = [existing_issue]
        else:
            mock_client.search_issues_by_keyword.return_value = []

        return patch("backlog_issue_cloner.BacklogClient", return_value=mock_client), mock_client

    def test_execute_creates_when_no_existing(self):
        """execute: 既存課題なし → ユーザーが y → create_issue が呼ばれる。"""
        patcher, mock_client = self._patch_client()
        with patcher, \
             patch("sys.stdout", new_callable=StringIO), \
             patch("builtins.input", return_value="y"):
            sut.run(_make_args(execute=True, date="20260828"), _make_config())
        mock_client.create_issue.assert_called_once()
        call_params = mock_client.create_issue.call_args[0][0]
        self.assertEqual(call_params["summary"], "【定期】20260828 タスク")
        self.assertEqual(call_params["description"], "本文テキスト")

    def test_execute_skips_when_user_cancels_create(self):
        """execute: 既存課題なし → ユーザーが n → create_issue は呼ばれない。"""
        patcher, mock_client = self._patch_client()
        with patcher, \
             patch("sys.stdout", new_callable=StringIO), \
             patch("builtins.input", return_value="n"):
            sut.run(_make_args(execute=True, date="20260828"), _make_config())
        mock_client.create_issue.assert_not_called()

    def test_execute_updates_when_desc_differs(self):
        """execute: 既存あり・本文差分あり → ユーザーが y → update_issue が呼ばれる。"""
        existing = {
            "issueKey": "PROJ-99",
            "summary": "【定期】20260828 タスク",
            "description": "古い本文",
        }
        patcher, mock_client = self._patch_client(existing_issue=existing)
        with patcher, \
             patch("sys.stdout", new_callable=StringIO), \
             patch("builtins.input", return_value="y"):
            sut.run(_make_args(execute=True, date="20260828"), _make_config())
        mock_client.update_issue.assert_called_once_with("PROJ-99", {"description": "本文テキスト"})

    def test_execute_skips_when_desc_same(self):
        """execute: 既存あり・本文同一 → 何もしない。"""
        existing = {
            "issueKey": "PROJ-99",
            "summary": "【定期】20260828 タスク",
            "description": "本文テキスト",  # SOURCE_ISSUE と同じ
        }
        patcher, mock_client = self._patch_client(existing_issue=existing)
        with patcher, patch("sys.stdout", new_callable=StringIO):
            sut.run(_make_args(execute=True, date="20260828"), _make_config())
        mock_client.create_issue.assert_not_called()
        mock_client.update_issue.assert_not_called()

    def test_project_key_derived_from_issue_key(self):
        """target_project_key 未設定時は issueKey プレフィックスから導出される。"""
        patcher, mock_client = self._patch_client()
        with patcher, \
             patch("sys.stdout", new_callable=StringIO), \
             patch("builtins.input", return_value="n"):
            sut.run(_make_args(execute=True, date="20260828"), _make_config())
        mock_client.get_project.assert_called_once_with("PROJ")

    def test_target_project_key_override(self):
        """target_project_key が設定されている場合はそちらを優先する。"""
        patcher, mock_client = self._patch_client()
        with patcher, \
             patch("sys.stdout", new_callable=StringIO), \
             patch("builtins.input", return_value="n"):
            sut.run(
                _make_args(execute=True, date="20260828"),
                _make_config(target_project_key="OTHER"),
            )
        mock_client.get_project.assert_called_once_with("OTHER")


# ===========================================================================
# validate_config テスト
# ===========================================================================


class TestValidateConfig(unittest.TestCase):
    def _base_config(self):
        return {
            "backlog": {
                "space_host": "test.backlog.com",
                "api_key": "REALKEY",
            },
            "clone": {
                "source_issue_key": "PROJ-1",
                "summary_template": "【定期】{YYYYMMDD} タスク",
            },
        }

    def test_valid_config_passes(self):
        sut.validate_config(self._base_config())  # 例外が出なければ OK

    def test_placeholder_api_key_exits(self):
        cfg = self._base_config()
        cfg["backlog"]["api_key"] = "YOUR_API_KEY_HERE"
        with self.assertRaises(SystemExit):
            sut.validate_config(cfg)

    def test_empty_api_key_exits(self):
        cfg = self._base_config()
        cfg["backlog"]["api_key"] = ""
        with self.assertRaises(SystemExit):
            sut.validate_config(cfg)

    def test_placeholder_space_host_exits(self):
        cfg = self._base_config()
        cfg["backlog"]["space_host"] = "yourcompany.backlog.com"
        with self.assertRaises(SystemExit):
            sut.validate_config(cfg)

    def test_placeholder_source_issue_key_exits(self):
        cfg = self._base_config()
        cfg["clone"]["source_issue_key"] = "PROJ-123"
        with self.assertRaises(SystemExit):
            sut.validate_config(cfg)

    def test_empty_summary_template_exits(self):
        cfg = self._base_config()
        cfg["clone"]["summary_template"] = ""
        with self.assertRaises(SystemExit):
            sut.validate_config(cfg)


if __name__ == "__main__":
    unittest.main(verbosity=2)
