"""
Backlog 課題クローンツール ユニットテスト
==========================================
BacklogClient をモック化して、API 接続なしで動作を検証する。
"""

import email.message
import io
import json
import pathlib
import tempfile
import unittest
import urllib.error
import urllib.parse
from contextlib import contextmanager
from io import StringIO
from unittest.mock import MagicMock, patch

# テスト対象モジュールのインポート
import backlog_issue_cloner as sut


# ===========================================================================
# テスト用ヘルパ
# ===========================================================================


@contextmanager
def tty():
    """確認プロンプトが対話環境として扱われるようにする。"""
    with patch("sys.stdin.isatty", return_value=True):
        yield


class FakeResponse:
    """urlopen の戻り値（コンテキストマネージャ）を模したオブジェクト。"""

    def __init__(self, payload):
        self._body = json.dumps(payload).encode("utf-8")

    def read(self):
        return self._body

    def __enter__(self):
        return self

    def __exit__(self, *exc_info):
        return False


def http_error(code, *, body=None, retry_after=None):
    headers = email.message.Message()
    if retry_after is not None:
        headers["Retry-After"] = str(retry_after)
    fp = io.BytesIO(json.dumps(body).encode("utf-8")) if body is not None else None
    return urllib.error.HTTPError(
        "https://test.backlog.com/api/v2/issues", code, "err", headers, fp
    )


# ===========================================================================
# find_existing_by_summary テスト
# ===========================================================================


class TestFindExistingBySummary(unittest.TestCase):
    """find_existing_by_summary — 件名フィルタの動作を検証。"""

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

    # --- match_mode="exact" ---

    def test_exact_mode_rejects_substring(self):
        """exact モードでは部分一致を既存扱いしない。"""
        issues = [{"issueKey": "PROJ-2", "summary": "【定期】20260828 タスク（再発）"}]
        client = self._make_client(issues)
        result = sut.find_existing_by_summary(
            client, 10, "【定期】20260828 タスク", match_mode="exact"
        )
        self.assertIsNone(result)

    def test_exact_mode_accepts_identical_summary(self):
        issues = [
            {"issueKey": "PROJ-2", "summary": "【定期】20260828 タスク（再発）"},
            {"issueKey": "PROJ-3", "summary": "【定期】20260828 タスク"},
        ]
        client = self._make_client(issues)
        result = sut.find_existing_by_summary(
            client, 10, "【定期】20260828 タスク", match_mode="exact"
        )
        self.assertEqual(result["issueKey"], "PROJ-3")

    # --- status_ids ---

    def test_status_ids_passed_through(self):
        client = self._make_client([])
        sut.find_existing_by_summary(
            client, 10, "件名", status_ids=sut.STATUS_IDS_OPEN
        )
        client.search_issues_by_keyword.assert_called_once_with(
            10, "件名", sut.STATUS_IDS_OPEN
        )


# ===========================================================================
# search_issues_by_keyword テスト
# ===========================================================================


class TestSearchIssuesPagination(unittest.TestCase):
    """search_issues_by_keyword — 遅延列挙による短絡と絞り込みを検証。"""

    TARGET = "【定期】20260828 タスク"

    def _client(self):
        return sut.BacklogClient(space_host="test.backlog.com", api_key="TESTKEY")

    @staticmethod
    def _page(n, summary="無関係な課題"):
        return [{"issueKey": f"PROJ-{i}", "summary": summary} for i in range(n)]

    def test_stops_at_first_page_when_match_found(self):
        """1ページ目にマッチがあれば2ページ目は取得しない。"""
        client = self._client()
        pages = [
            self._page(99) + [{"issueKey": "PROJ-HIT", "summary": self.TARGET}],
            self._page(100),
        ]
        with patch.object(client, "_get", side_effect=pages) as mock_get, \
             patch("backlog_issue_cloner.time.sleep") as mock_sleep:
            result = sut.find_existing_by_summary(client, 10, self.TARGET)
        self.assertEqual(result["issueKey"], "PROJ-HIT")
        self.assertEqual(mock_get.call_count, 1)
        mock_sleep.assert_not_called()

    def test_fetches_next_page_when_no_match_on_first(self):
        """1ページ目が満杯かつ未ヒットなら次ページを取得する。"""
        client = self._client()
        pages = [
            self._page(100),
            [{"issueKey": "PROJ-HIT", "summary": self.TARGET}],
        ]
        with patch.object(client, "_get", side_effect=pages) as mock_get, \
             patch("backlog_issue_cloner.time.sleep"):
            result = sut.find_existing_by_summary(client, 10, self.TARGET)
        self.assertEqual(result["issueKey"], "PROJ-HIT")
        self.assertEqual(mock_get.call_count, 2)
        self.assertEqual(mock_get.call_args_list[1][0][1]["offset"], 100)

    def test_stops_when_page_is_not_full(self):
        """満杯でないページで打ち切り、マッチしなければ None。"""
        client = self._client()
        with patch.object(client, "_get", side_effect=[self._page(5)]) as mock_get, \
             patch("backlog_issue_cloner.time.sleep"):
            result = sut.find_existing_by_summary(client, 10, self.TARGET)
        self.assertIsNone(result)
        self.assertEqual(mock_get.call_count, 1)

    def test_empty_first_page_returns_none(self):
        client = self._client()
        with patch.object(client, "_get", side_effect=[[]]) as mock_get:
            result = sut.find_existing_by_summary(client, 10, self.TARGET)
        self.assertIsNone(result)
        self.assertEqual(mock_get.call_count, 1)

    def test_status_ids_included_in_request(self):
        """status_ids を渡すとクエリパラメータに載る。"""
        client = self._client()
        with patch.object(client, "_get", side_effect=[[]]) as mock_get:
            list(client.search_issues_by_keyword(10, "件名", [1, 2, 3]))
        self.assertEqual(mock_get.call_args[0][1]["statusId"], [1, 2, 3])

    def test_status_ids_omitted_when_none(self):
        client = self._client()
        with patch.object(client, "_get", side_effect=[[]]) as mock_get:
            list(client.search_issues_by_keyword(10, "件名"))
        self.assertNotIn("statusId", mock_get.call_args[0][1])


# ===========================================================================
# パラメータ展開テスト
# ===========================================================================


class TestFlattenParams(unittest.TestCase):
    """_flatten_params — GET のクエリと POST のボディで共用する展開処理。"""

    def test_scalar_values(self):
        self.assertEqual(
            sut._flatten_params({"keyword": "件名", "count": 100}),
            [("keyword", "件名"), ("count", "100")],
        )

    def test_list_values_expanded_with_brackets(self):
        self.assertEqual(
            sut._flatten_params({"statusId": [1, 2, 3]}),
            [("statusId[]", "1"), ("statusId[]", "2"), ("statusId[]", "3")],
        )

    def test_mixed(self):
        self.assertEqual(
            sut._flatten_params({"projectId": [10], "offset": 0}),
            [("projectId[]", "10"), ("offset", "0")],
        )

    def test_empty_list_produces_no_pair(self):
        self.assertEqual(sut._flatten_params({"statusId": []}), [])

    def test_query_encoding_round_trips(self):
        """クエリ文字列に組み立てた後、パースして元の値に戻ることを確認する。"""
        params = {"statusId": [1, 2], "keyword": "a/b c&d=e 【定期】"}
        query = urllib.parse.urlencode(
            sut._flatten_params(params), quote_via=urllib.parse.quote
        )
        parsed = urllib.parse.parse_qs(query)
        self.assertEqual(parsed["statusId[]"], ["1", "2"])
        self.assertEqual(parsed["keyword"], ["a/b c&d=e 【定期】"])

    def test_body_encoding_round_trips(self):
        params = {"summary": "件名 テスト", "description": "改行\nあり"}
        body = urllib.parse.urlencode(sut._flatten_params(params))
        parsed = urllib.parse.parse_qs(body)
        self.assertEqual(parsed["summary"], ["件名 テスト"])
        self.assertEqual(parsed["description"], ["改行\nあり"])


# ===========================================================================
# リトライ動作テスト
# ===========================================================================


class TestRetry(unittest.TestCase):
    """_request — 429 / 5xx / ネットワークエラーの再試行を検証。"""

    def _client(self, **kw):
        kw.setdefault("max_retries", 3)
        kw.setdefault("retry_backoff", 1.0)
        return sut.BacklogClient(
            space_host="test.backlog.com", api_key="TESTKEY", **kw
        )

    @contextmanager
    def _urlopen(self, side_effect):
        with patch("backlog_issue_cloner.urllib.request.urlopen",
                   side_effect=side_effect) as mock_open, \
             patch("backlog_issue_cloner.time.sleep") as mock_sleep, \
             patch("sys.stderr", new_callable=StringIO):
            yield mock_open, mock_sleep

    def test_retries_on_429_then_succeeds(self):
        client = self._client()
        with self._urlopen([http_error(429), FakeResponse({"id": 1})]) as (op, sleep):
            result = client.get_priorities()
        self.assertEqual(result, {"id": 1})
        self.assertEqual(op.call_count, 2)
        sleep.assert_called_once_with(1.0)

    def test_retries_on_503_then_succeeds(self):
        client = self._client()
        with self._urlopen([http_error(503), FakeResponse({"id": 2})]) as (op, _):
            result = client.get_priorities()
        self.assertEqual(result, {"id": 2})
        self.assertEqual(op.call_count, 2)

    def test_exponential_backoff(self):
        """待機秒数が 1 → 2 → 4 と倍増する。"""
        client = self._client()
        errors = [http_error(500)] * 4  # 初回 + リトライ3回すべて失敗
        with self._urlopen(errors) as (op, sleep):
            with self.assertRaises(sut.BacklogError):
                client.get_priorities()
        self.assertEqual(op.call_count, 4)
        self.assertEqual([c[0][0] for c in sleep.call_args_list], [1.0, 2.0, 4.0])

    def test_retry_after_header_takes_precedence(self):
        client = self._client()
        with self._urlopen([http_error(429, retry_after=7),
                            FakeResponse({})]) as (_, sleep):
            client.get_priorities()
        sleep.assert_called_once_with(7.0)

    def test_retry_after_capped_at_max_delay(self):
        client = self._client(retry_max_delay=30.0)
        with self._urlopen([http_error(429, retry_after=999),
                            FakeResponse({})]) as (_, sleep):
            client.get_priorities()
        sleep.assert_called_once_with(30.0)

    def test_invalid_retry_after_falls_back_to_backoff(self):
        """HTTP-date 形式の Retry-After は解釈できないので指数バックオフを使う。"""
        client = self._client()
        with self._urlopen([http_error(429, retry_after="Wed, 21 Oct 2026 07:28:00 GMT"),
                            FakeResponse({})]) as (_, sleep):
            client.get_priorities()
        sleep.assert_called_once_with(1.0)

    def test_no_retry_on_401(self):
        """認証エラーは再試行せず即座に BacklogError。"""
        client = self._client()
        err = http_error(401, body={"errors": [{"message": "認証失敗", "code": 11}]})
        with self._urlopen([err]) as (op, sleep):
            with self.assertRaises(sut.BacklogError) as ctx:
                client.get_priorities()
        self.assertEqual(op.call_count, 1)
        sleep.assert_not_called()
        self.assertEqual(ctx.exception.status, 401)
        self.assertIn("api_key を確認してください。", ctx.exception.hint)

    def test_retries_on_network_error(self):
        client = self._client()
        errors = [urllib.error.URLError("接続拒否"), FakeResponse({"ok": True})]
        with self._urlopen(errors) as (op, _):
            result = client.get_priorities()
        self.assertEqual(result, {"ok": True})
        self.assertEqual(op.call_count, 2)

    def test_retries_on_timeout(self):
        """レスポンス待ちのタイムアウトは TimeoutError で送出されるため個別に検証する。"""
        client = self._client()
        errors = [TimeoutError("timed out"), FakeResponse({"ok": True})]
        with self._urlopen(errors) as (op, _):
            result = client.get_priorities()
        self.assertEqual(result, {"ok": True})
        self.assertEqual(op.call_count, 2)

    def test_timeout_exhausted_raises_backlog_error(self):
        client = self._client()
        with self._urlopen([TimeoutError("timed out")] * 4) as (op, _):
            with self.assertRaises(sut.BacklogError) as ctx:
                client.get_priorities()
        self.assertEqual(op.call_count, 4)
        self.assertIn("ネットワークエラー", str(ctx.exception))
        self.assertIn("timed out", str(ctx.exception))

    def test_network_error_exhausted_raises_backlog_error(self):
        client = self._client()
        errors = [urllib.error.URLError("接続拒否")] * 4
        with self._urlopen(errors) as (op, _):
            with self.assertRaises(sut.BacklogError) as ctx:
                client.get_priorities()
        self.assertEqual(op.call_count, 4)
        self.assertIn("ネットワークエラー", str(ctx.exception))

    def test_retries_disabled(self):
        client = self._client(max_retries=0)
        with self._urlopen([http_error(503)]) as (op, sleep):
            with self.assertRaises(sut.BacklogError):
                client.get_priorities()
        self.assertEqual(op.call_count, 1)
        sleep.assert_not_called()

    def test_404_with_allow_404_returns_none_without_retry(self):
        client = self._client()
        with self._urlopen([http_error(404)]) as (op, sleep):
            result = client.get_issue("PROJ-999")
        self.assertIsNone(result)
        self.assertEqual(op.call_count, 1)
        sleep.assert_not_called()

    # --- 更新系リクエスト（POST / PATCH）のリトライ制限 ---

    def _create(self, client):
        return client.create_issue({"projectId": 1, "summary": "件名"})

    def test_post_retries_on_429(self):
        """429 は拒否＝未処理が確実なので更新系でも再試行する。"""
        client = self._client()
        with self._urlopen([http_error(429), FakeResponse({"issueKey": "P-1"})]) as (op, _):
            result = self._create(client)
        self.assertEqual(result, {"issueKey": "P-1"})
        self.assertEqual(op.call_count, 2)

    def test_post_does_not_retry_on_503(self):
        """5xx は処理済みの可能性があるため更新系では再試行しない。"""
        client = self._client()
        with self._urlopen([http_error(503)] * 4) as (op, sleep):
            with self.assertRaises(sut.BacklogError):
                self._create(client)
        self.assertEqual(op.call_count, 1)
        sleep.assert_not_called()

    def test_post_does_not_retry_on_timeout(self):
        """タイムアウトは送信後に失敗した可能性があるため再試行しない。"""
        client = self._client()
        with self._urlopen([TimeoutError("timed out")] * 4) as (op, sleep):
            with self.assertRaises(sut.BacklogError) as ctx:
                self._create(client)
        self.assertEqual(op.call_count, 1)
        sleep.assert_not_called()
        self.assertIn("再試行していません", ctx.exception.hint)

    def test_post_does_not_retry_on_connection_reset(self):
        """接続断も送信後に失敗した可能性があるため再試行しない。"""
        client = self._client()
        with self._urlopen([ConnectionResetError("reset")] * 4) as (op, sleep):
            with self.assertRaises(sut.BacklogError):
                self._create(client)
        self.assertEqual(op.call_count, 1)
        sleep.assert_not_called()

    def test_post_retries_on_url_error(self):
        """接続自体に失敗した場合はサーバに届いていないため再試行する。"""
        client = self._client()
        errors = [urllib.error.URLError("接続拒否"), FakeResponse({"issueKey": "P-1"})]
        with self._urlopen(errors) as (op, _):
            result = self._create(client)
        self.assertEqual(result, {"issueKey": "P-1"})
        self.assertEqual(op.call_count, 2)

    def test_patch_does_not_retry_on_503(self):
        client = self._client()
        with self._urlopen([http_error(503)] * 4) as (op, _):
            with self.assertRaises(sut.BacklogError):
                client.update_issue("P-1", {"description": "本文"})
        self.assertEqual(op.call_count, 1)

    def test_get_still_retries_on_503(self):
        """GET は冪等なので従来どおり再試行する。"""
        client = self._client()
        with self._urlopen([http_error(503), FakeResponse({"ok": True})]) as (op, _):
            result = client.get_priorities()
        self.assertEqual(result, {"ok": True})
        self.assertEqual(op.call_count, 2)

    def test_get_still_retries_on_connection_reset(self):
        client = self._client()
        errors = [ConnectionResetError("reset"), FakeResponse({"ok": True})]
        with self._urlopen(errors) as (op, _):
            result = client.get_priorities()
        self.assertEqual(result, {"ok": True})
        self.assertEqual(op.call_count, 2)

    def test_no_change_error_raised_on_code_7(self):
        client = self._client()
        err = http_error(400, body={"errors": [{"message": "変更なし", "code": 7}]})
        with self._urlopen([err]):
            with self.assertRaises(sut.BacklogNoChangeError):
                client.update_issue("PROJ-1", {"description": "本文"})


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

    def test_invalid_date_raises_config_error(self):
        with self.assertRaises(sut.ConfigError):
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

    def test_empty_types_raises_config_error(self):
        client = MagicMock()
        client.get_issue_types.return_value = []
        with self.assertRaises(sut.ConfigError):
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

    def test_warns_and_falls_back_to_chuu_when_not_found(self):
        """指定した優先度が見つからない場合は警告して「中」を使う。"""
        client = self._make_client()
        with patch("sys.stderr", new_callable=StringIO) as err:
            id_, name = sut.resolve_priority_id(client, "存在しない優先度")
        self.assertEqual(id_, 3)
        self.assertEqual(name, "中")
        message = err.getvalue()
        self.assertIn("存在しない優先度", message)
        self.assertIn("高", message)  # 利用可能な値を案内する

    def test_falls_back_to_first_when_not_found_and_no_chuu(self):
        priorities = [{"id": 2, "name": "高"}, {"id": 4, "name": "低"}]
        client = self._make_client(priorities)
        with patch("sys.stderr", new_callable=StringIO):
            id_, name = sut.resolve_priority_id(client, "存在しない優先度")
        self.assertEqual(id_, 2)
        self.assertEqual(name, "高")

    def test_fallback_to_first_when_chuu_not_found(self):
        priorities = [{"id": 2, "name": "高"}, {"id": 4, "name": "低"}]
        client = self._make_client(priorities)
        id_, name = sut.resolve_priority_id(client, None)
        self.assertEqual(id_, 2)
        self.assertEqual(name, "高")

    def test_empty_priorities_raises_config_error(self):
        client = self._make_client([])
        with self.assertRaises(sut.ConfigError):
            sut.resolve_priority_id(client, None)


# ===========================================================================
# 確認プロンプトテスト
# ===========================================================================


class TestConfirm(unittest.TestCase):
    def test_assume_yes_skips_input(self):
        with patch("sys.stdout", new_callable=StringIO), \
             patch("builtins.input", side_effect=AssertionError("input が呼ばれた")):
            self.assertTrue(sut.confirm_create("件名", "PROJ-1", "本文", assume_yes=True))

    def test_non_interactive_without_yes_returns_false(self):
        """非対話環境で --yes なしなら input を呼ばずに False。"""
        with patch("sys.stdout", new_callable=StringIO), \
             patch("sys.stderr", new_callable=StringIO) as err, \
             patch("sys.stdin.isatty", return_value=False), \
             patch("builtins.input", side_effect=AssertionError("input が呼ばれた")):
            result = sut.confirm_create("件名", "PROJ-1", "本文")
        self.assertFalse(result)
        self.assertIn("--yes", err.getvalue())

    def test_interactive_yes(self):
        with patch("sys.stdout", new_callable=StringIO), tty(), \
             patch("builtins.input", return_value="y"):
            self.assertTrue(sut.confirm_update("PROJ-1", "旧", "新"))

    def test_interactive_no(self):
        with patch("sys.stdout", new_callable=StringIO), tty(), \
             patch("builtins.input", return_value="n"):
            self.assertFalse(sut.confirm_update("PROJ-1", "旧", "新"))

    def test_eof_treated_as_no(self):
        with patch("sys.stdout", new_callable=StringIO), tty(), \
             patch("builtins.input", side_effect=EOFError):
            self.assertFalse(sut.confirm_update("PROJ-1", "旧", "新"))


# ===========================================================================
# 終了コードテスト
# ===========================================================================


class TestExitCode(unittest.TestCase):
    def test_default_returns_zero_for_all_success(self):
        for outcome in (sut.OUTCOME_NO_CHANGE, sut.OUTCOME_CREATED, sut.OUTCOME_UPDATED):
            self.assertEqual(sut.exit_code_for(outcome, detailed=False), 0)

    def test_skipped_is_non_zero_even_by_default(self):
        self.assertEqual(sut.exit_code_for(sut.OUTCOME_SKIPPED, detailed=False), 20)

    def test_detailed_distinguishes_outcomes(self):
        self.assertEqual(sut.exit_code_for(sut.OUTCOME_NO_CHANGE, detailed=True), 0)
        self.assertEqual(sut.exit_code_for(sut.OUTCOME_CREATED, detailed=True), 10)
        self.assertEqual(sut.exit_code_for(sut.OUTCOME_UPDATED, detailed=True), 11)

    def test_detailed_skipped_still_twenty(self):
        self.assertEqual(sut.exit_code_for(sut.OUTCOME_SKIPPED, detailed=True), 20)


# ===========================================================================
# run() 統合テスト（BacklogClient 全体をモック）
# ===========================================================================


def _make_args(execute=False, date=None, debug=False, config="config.yaml", yes=False):
    args = MagicMock()
    args.execute = execute
    args.date = date
    args.debug = debug
    args.config = config
    args.yes = yes
    return args


def _make_config(
    space_host="test.backlog.com",
    api_key="TESTKEY",
    source_issue_key="PROJ-1",
    summary_template="【定期】{YYYYMMDD} タスク",
    target_project_key=None,
    issue_type=None,
    priority=None,
    match_mode=None,
    include_closed=None,
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
    if match_mode:
        cfg["clone"]["match_mode"] = match_mode
    if include_closed is not None:
        cfg["clone"]["include_closed"] = include_closed
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


def _mock_client(existing_issue=None):
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
    mock_client.search_issues_by_keyword.return_value = (
        [existing_issue] if existing_issue else []
    )
    return patch("backlog_issue_cloner.BacklogClient", return_value=mock_client), mock_client


EXISTING_SAME = {
    "issueKey": "PROJ-99",
    "summary": "【定期】20260828 タスク",
    "description": "本文テキスト",
}
EXISTING_DIFF = {
    "issueKey": "PROJ-99",
    "summary": "【定期】20260828 タスク",
    "description": "古い本文",
}


class TestRunDryRun(unittest.TestCase):
    """ドライランモードでは API 書き込みが発生しないことを検証。"""

    def test_dry_run_no_existing_no_create(self):
        """ドライラン: 既存課題なし → create_issue は呼ばれず created を返す。"""
        patcher, mock_client = _mock_client()
        with patcher, patch("sys.stdout", new_callable=StringIO):
            outcome = sut.run(_make_args(execute=False, date="20260828"), _make_config())
        mock_client.create_issue.assert_not_called()
        self.assertEqual(outcome, sut.OUTCOME_CREATED)

    def test_dry_run_existing_same_desc_no_update(self):
        """ドライラン: 既存課題あり・本文同一 → update_issue は呼ばれない。"""
        patcher, mock_client = _mock_client(existing_issue=EXISTING_SAME)
        with patcher, patch("sys.stdout", new_callable=StringIO):
            outcome = sut.run(_make_args(execute=False, date="20260828"), _make_config())
        mock_client.update_issue.assert_not_called()
        self.assertEqual(outcome, sut.OUTCOME_NO_CHANGE)

    def test_dry_run_existing_diff_desc_no_update(self):
        """ドライラン: 既存課題あり・本文差分あり → update_issue は呼ばれない。"""
        patcher, mock_client = _mock_client(existing_issue=EXISTING_DIFF)
        with patcher, patch("sys.stdout", new_callable=StringIO):
            outcome = sut.run(_make_args(execute=False, date="20260828"), _make_config())
        mock_client.update_issue.assert_not_called()
        self.assertEqual(outcome, sut.OUTCOME_UPDATED)


class TestRunExecute(unittest.TestCase):
    """--execute モードでの作成・更新・スキップを検証。"""

    def test_execute_creates_when_no_existing(self):
        """execute: 既存課題なし → ユーザーが y → create_issue が呼ばれる。"""
        patcher, mock_client = _mock_client()
        with patcher, patch("sys.stdout", new_callable=StringIO), tty(), \
             patch("builtins.input", return_value="y"):
            outcome = sut.run(_make_args(execute=True, date="20260828"), _make_config())
        mock_client.create_issue.assert_called_once()
        call_params = mock_client.create_issue.call_args[0][0]
        self.assertEqual(call_params["summary"], "【定期】20260828 タスク")
        self.assertEqual(call_params["description"], "本文テキスト")
        self.assertEqual(outcome, sut.OUTCOME_CREATED)

    def test_execute_skips_when_user_cancels_create(self):
        """execute: 既存課題なし → ユーザーが n → create_issue は呼ばれない。"""
        patcher, mock_client = _mock_client()
        with patcher, patch("sys.stdout", new_callable=StringIO), tty(), \
             patch("builtins.input", return_value="n"):
            outcome = sut.run(_make_args(execute=True, date="20260828"), _make_config())
        mock_client.create_issue.assert_not_called()
        self.assertEqual(outcome, sut.OUTCOME_SKIPPED)

    def test_execute_updates_when_desc_differs(self):
        """execute: 既存あり・本文差分あり → ユーザーが y → update_issue が呼ばれる。"""
        patcher, mock_client = _mock_client(existing_issue=EXISTING_DIFF)
        with patcher, patch("sys.stdout", new_callable=StringIO), tty(), \
             patch("builtins.input", return_value="y"):
            outcome = sut.run(_make_args(execute=True, date="20260828"), _make_config())
        mock_client.update_issue.assert_called_once_with(
            "PROJ-99", {"description": "本文テキスト"}
        )
        self.assertEqual(outcome, sut.OUTCOME_UPDATED)

    def test_execute_skips_when_user_cancels_update(self):
        """execute: 既存あり・本文差分あり → ユーザーが n → update_issue は呼ばれない。"""
        patcher, mock_client = _mock_client(existing_issue=EXISTING_DIFF)
        with patcher, patch("sys.stdout", new_callable=StringIO), tty(), \
             patch("builtins.input", return_value="n"):
            outcome = sut.run(_make_args(execute=True, date="20260828"), _make_config())
        mock_client.update_issue.assert_not_called()
        self.assertEqual(outcome, sut.OUTCOME_SKIPPED)

    def test_execute_skips_update_when_non_interactive_without_yes(self):
        patcher, mock_client = _mock_client(existing_issue=EXISTING_DIFF)
        with patcher, patch("sys.stdout", new_callable=StringIO), \
             patch("sys.stderr", new_callable=StringIO), \
             patch("sys.stdin.isatty", return_value=False):
            outcome = sut.run(_make_args(execute=True, date="20260828"), _make_config())
        mock_client.update_issue.assert_not_called()
        self.assertEqual(outcome, sut.OUTCOME_SKIPPED)

    def test_yes_updates_without_prompt(self):
        patcher, mock_client = _mock_client(existing_issue=EXISTING_DIFF)
        with patcher, patch("sys.stdout", new_callable=StringIO), \
             patch("sys.stdin.isatty", return_value=False), \
             patch("builtins.input", side_effect=AssertionError("input が呼ばれた")):
            outcome = sut.run(
                _make_args(execute=True, date="20260828", yes=True), _make_config()
            )
        mock_client.update_issue.assert_called_once()
        self.assertEqual(outcome, sut.OUTCOME_UPDATED)

    def test_execute_skips_when_desc_same(self):
        """execute: 既存あり・本文同一 → 何もしない。"""
        patcher, mock_client = _mock_client(existing_issue=EXISTING_SAME)
        with patcher, patch("sys.stdout", new_callable=StringIO):
            outcome = sut.run(_make_args(execute=True, date="20260828"), _make_config())
        mock_client.create_issue.assert_not_called()
        mock_client.update_issue.assert_not_called()
        self.assertEqual(outcome, sut.OUTCOME_NO_CHANGE)

    def test_no_change_error_is_treated_as_no_change(self):
        """更新時に BacklogNoChangeError が出たら no_change 扱い。"""
        patcher, mock_client = _mock_client(existing_issue=EXISTING_DIFF)
        mock_client.update_issue.side_effect = sut.BacklogNoChangeError("変更なし")
        with patcher, patch("sys.stdout", new_callable=StringIO), tty(), \
             patch("builtins.input", return_value="y"):
            outcome = sut.run(_make_args(execute=True, date="20260828"), _make_config())
        self.assertEqual(outcome, sut.OUTCOME_NO_CHANGE)

    # --- --yes ---

    def test_yes_creates_without_prompt(self):
        """--yes: 非対話環境でも input なしで作成する。"""
        patcher, mock_client = _mock_client()
        with patcher, patch("sys.stdout", new_callable=StringIO), \
             patch("sys.stdin.isatty", return_value=False), \
             patch("builtins.input", side_effect=AssertionError("input が呼ばれた")):
            outcome = sut.run(
                _make_args(execute=True, date="20260828", yes=True), _make_config()
            )
        mock_client.create_issue.assert_called_once()
        self.assertEqual(outcome, sut.OUTCOME_CREATED)

    def test_without_yes_non_interactive_skips(self):
        """--yes なしの非対話実行は作成せず skipped を返す。"""
        patcher, mock_client = _mock_client()
        with patcher, patch("sys.stdout", new_callable=StringIO), \
             patch("sys.stderr", new_callable=StringIO), \
             patch("sys.stdin.isatty", return_value=False):
            outcome = sut.run(_make_args(execute=True, date="20260828"), _make_config())
        mock_client.create_issue.assert_not_called()
        self.assertEqual(outcome, sut.OUTCOME_SKIPPED)

    # --- 種別・優先度の遅延解決（不要な API 呼び出しの抑止） ---

    def test_no_change_skips_type_and_priority_lookup(self):
        """変更なしの経路では種別・優先度を取得しない。"""
        patcher, mock_client = _mock_client(existing_issue=EXISTING_SAME)
        with patcher, patch("sys.stdout", new_callable=StringIO):
            sut.run(_make_args(execute=True, date="20260828"), _make_config())
        mock_client.get_issue_types.assert_not_called()
        mock_client.get_priorities.assert_not_called()

    def test_update_skips_type_and_priority_lookup(self):
        """更新の経路でも種別・優先度は使わないので取得しない。"""
        patcher, mock_client = _mock_client(existing_issue=EXISTING_DIFF)
        with patcher, patch("sys.stdout", new_callable=StringIO), tty(), \
             patch("builtins.input", return_value="y"):
            sut.run(_make_args(execute=True, date="20260828"), _make_config())
        mock_client.update_issue.assert_called_once()
        mock_client.get_issue_types.assert_not_called()
        mock_client.get_priorities.assert_not_called()

    def test_create_resolves_type_and_priority(self):
        """新規作成の経路では種別・優先度を取得して作成パラメータに含める。"""
        patcher, mock_client = _mock_client()
        with patcher, patch("sys.stdout", new_callable=StringIO), tty(), \
             patch("builtins.input", return_value="y"):
            sut.run(_make_args(execute=True, date="20260828"), _make_config())
        mock_client.get_issue_types.assert_called_once()
        mock_client.get_priorities.assert_called_once()
        params = mock_client.create_issue.call_args[0][0]
        self.assertEqual(params["issueTypeId"], 1)
        self.assertEqual(params["priorityId"], 3)

    def test_dry_run_create_still_resolves_type_and_priority(self):
        """ドライランでも作成予定なら種別名の存在確認のため解決する。"""
        patcher, mock_client = _mock_client()
        with patcher, patch("sys.stdout", new_callable=StringIO):
            outcome = sut.run(_make_args(execute=False, date="20260828"), _make_config())
        self.assertEqual(outcome, sut.OUTCOME_CREATED)
        mock_client.get_issue_types.assert_called_once()
        mock_client.get_priorities.assert_called_once()
        mock_client.create_issue.assert_not_called()

    def test_dry_run_no_change_skips_lookup(self):
        patcher, mock_client = _mock_client(existing_issue=EXISTING_SAME)
        with patcher, patch("sys.stdout", new_callable=StringIO):
            sut.run(_make_args(execute=False, date="20260828"), _make_config())
        mock_client.get_issue_types.assert_not_called()
        mock_client.get_priorities.assert_not_called()

    # --- プロジェクト解決 ---

    def test_project_id_reused_from_source_issue(self):
        """target_project_key 未設定時は source_issue の projectId を流用する。"""
        patcher, mock_client = _mock_client()
        with patcher, patch("sys.stdout", new_callable=StringIO), tty(), \
             patch("builtins.input", return_value="n"):
            sut.run(_make_args(execute=True, date="20260828"), _make_config())
        mock_client.get_project.assert_not_called()
        mock_client.search_issues_by_keyword.assert_called_once_with(
            10, "【定期】20260828 タスク", sut.STATUS_IDS_OPEN
        )

    def test_project_key_derived_when_project_id_missing(self):
        """projectId が無い場合は issueKey プレフィックスで get_project にフォールバック。"""
        patcher, mock_client = _mock_client()
        mock_client.get_issue.return_value = {
            k: v for k, v in SOURCE_ISSUE.items() if k != "projectId"
        }
        with patcher, patch("sys.stdout", new_callable=StringIO), tty(), \
             patch("builtins.input", return_value="n"):
            sut.run(_make_args(execute=True, date="20260828"), _make_config())
        mock_client.get_project.assert_called_once_with("PROJ")

    def test_target_project_key_override(self):
        """target_project_key が設定されている場合はそちらを優先する。"""
        patcher, mock_client = _mock_client()
        with patcher, patch("sys.stdout", new_callable=StringIO), tty(), \
             patch("builtins.input", return_value="n"):
            sut.run(
                _make_args(execute=True, date="20260828"),
                _make_config(target_project_key="OTHER"),
            )
        mock_client.get_project.assert_called_once_with("OTHER")

    # --- 重複判定オプション ---

    def test_include_closed_disables_status_filter(self):
        patcher, mock_client = _mock_client()
        with patcher, patch("sys.stdout", new_callable=StringIO), tty(), \
             patch("builtins.input", return_value="n"):
            sut.run(
                _make_args(execute=True, date="20260828"),
                _make_config(include_closed=True),
            )
        mock_client.search_issues_by_keyword.assert_called_once_with(
            10, "【定期】20260828 タスク", None
        )

    def test_exact_match_mode_creates_when_only_substring_exists(self):
        """exact モードでは部分一致の既存課題があっても新規作成する。"""
        near_miss = {
            "issueKey": "PROJ-99",
            "summary": "【定期】20260828 タスク（再発）",
            "description": "別の本文",
        }
        patcher, mock_client = _mock_client(existing_issue=near_miss)
        with patcher, patch("sys.stdout", new_callable=StringIO), tty(), \
             patch("builtins.input", return_value="y"):
            outcome = sut.run(
                _make_args(execute=True, date="20260828"),
                _make_config(match_mode="exact"),
            )
        mock_client.create_issue.assert_called_once()
        mock_client.update_issue.assert_not_called()
        self.assertEqual(outcome, sut.OUTCOME_CREATED)

    def test_source_issue_missing_raises_config_error(self):
        patcher, mock_client = _mock_client()
        mock_client.get_issue.return_value = None
        with patcher, patch("sys.stdout", new_callable=StringIO):
            with self.assertRaises(sut.ConfigError):
                sut.run(_make_args(execute=True, date="20260828"), _make_config())


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

    def test_placeholder_api_key_raises(self):
        cfg = self._base_config()
        cfg["backlog"]["api_key"] = "YOUR_API_KEY_HERE"
        with self.assertRaises(sut.ConfigError):
            sut.validate_config(cfg)

    def test_empty_api_key_raises(self):
        cfg = self._base_config()
        cfg["backlog"]["api_key"] = ""
        with self.assertRaises(sut.ConfigError):
            sut.validate_config(cfg)

    def test_placeholder_space_host_raises(self):
        cfg = self._base_config()
        cfg["backlog"]["space_host"] = "yourcompany.backlog.com"
        with self.assertRaises(sut.ConfigError):
            sut.validate_config(cfg)

    def test_placeholder_source_issue_key_raises(self):
        cfg = self._base_config()
        cfg["clone"]["source_issue_key"] = "PROJ-123"
        with self.assertRaises(sut.ConfigError):
            sut.validate_config(cfg)

    def test_empty_summary_template_raises(self):
        cfg = self._base_config()
        cfg["clone"]["summary_template"] = ""
        with self.assertRaises(sut.ConfigError):
            sut.validate_config(cfg)

    def test_invalid_match_mode_raises(self):
        cfg = self._base_config()
        cfg["clone"]["match_mode"] = "regex"
        with self.assertRaises(sut.ConfigError):
            sut.validate_config(cfg)

    def test_valid_match_modes_pass(self):
        for mode in sut.MATCH_MODES:
            cfg = self._base_config()
            cfg["clone"]["match_mode"] = mode
            sut.validate_config(cfg)

    # --- 空・型不正の設定ファイル ---

    def test_none_config_raises(self):
        """空の YAML は safe_load が None を返す。"""
        with self.assertRaises(sut.ConfigError):
            sut.validate_config(None)

    def test_missing_backlog_section_raises(self):
        with self.assertRaises(sut.ConfigError):
            sut.validate_config({"clone": {}})

    def test_null_backlog_section_raises(self):
        """`backlog:` と書いて中身が空のケース。"""
        cfg = self._base_config()
        cfg["backlog"] = None
        with self.assertRaises(sut.ConfigError):
            sut.validate_config(cfg)

    def test_null_clone_section_raises(self):
        cfg = self._base_config()
        cfg["clone"] = None
        with self.assertRaises(sut.ConfigError):
            sut.validate_config(cfg)

    def test_non_mapping_section_raises(self):
        cfg = self._base_config()
        cfg["backlog"] = ["これはリスト"]
        with self.assertRaises(sut.ConfigError):
            sut.validate_config(cfg)

    # --- 数値設定 ---

    def test_numeric_settings_accept_valid_values(self):
        cfg = self._base_config()
        cfg["backlog"].update(
            timeout=10, max_retries=0, retry_backoff=0.5, retry_max_delay=30
        )
        sut.validate_config(cfg)

    def test_negative_numeric_setting_raises(self):
        cfg = self._base_config()
        cfg["backlog"]["max_retries"] = -1
        with self.assertRaises(sut.ConfigError):
            sut.validate_config(cfg)

    def test_zero_timeout_raises(self):
        cfg = self._base_config()
        cfg["backlog"]["timeout"] = 0
        with self.assertRaises(sut.ConfigError):
            sut.validate_config(cfg)

    def test_string_numeric_setting_raises(self):
        cfg = self._base_config()
        cfg["backlog"]["max_retries"] = "3"
        with self.assertRaises(sut.ConfigError):
            sut.validate_config(cfg)

    def test_bool_numeric_setting_raises(self):
        """bool は int のサブクラスだが数値設定としては受け付けない。"""
        cfg = self._base_config()
        cfg["backlog"]["max_retries"] = True
        with self.assertRaises(sut.ConfigError):
            sut.validate_config(cfg)

    # --- 真偽値設定 ---

    def test_bool_settings_accept_bool(self):
        cfg = self._base_config()
        cfg["backlog"]["ssl_verify"] = False
        cfg["clone"]["include_closed"] = True
        sut.validate_config(cfg)

    def test_string_false_for_include_closed_raises(self):
        """"false" という文字列は真と評価されてしまうため弾く。"""
        cfg = self._base_config()
        cfg["clone"]["include_closed"] = "false"
        with self.assertRaises(sut.ConfigError):
            sut.validate_config(cfg)

    def test_string_for_ssl_verify_raises(self):
        cfg = self._base_config()
        cfg["backlog"]["ssl_verify"] = "true"
        with self.assertRaises(sut.ConfigError):
            sut.validate_config(cfg)


# ===========================================================================
# 設定ファイルの読み込み・BacklogClient への受け渡し
# ===========================================================================


class TestClientSettingsFromConfig(unittest.TestCase):
    """timeout / retry 系の設定が BacklogClient に渡ることを検証。"""

    def _run_and_capture_kwargs(self, backlog_overrides):
        cfg = _make_config()
        cfg["backlog"].update(backlog_overrides)
        patcher, mock_client = _mock_client()
        with patcher as mock_cls, patch("sys.stdout", new_callable=StringIO), \
             patch("sys.stderr", new_callable=StringIO), \
             patch("sys.stdin.isatty", return_value=False):
            sut.run(_make_args(execute=True, date="20260828"), cfg)
        return mock_cls.call_args[1]

    def test_defaults_applied_when_absent(self):
        kwargs = self._run_and_capture_kwargs({})
        self.assertEqual(kwargs["timeout"], 30)
        self.assertEqual(kwargs["max_retries"], 3)
        self.assertEqual(kwargs["retry_backoff"], 1.0)
        self.assertEqual(kwargs["retry_max_delay"], 60.0)

    def test_config_values_override_defaults(self):
        kwargs = self._run_and_capture_kwargs({
            "timeout": 5,
            "max_retries": 0,
            "retry_backoff": 0.25,
            "retry_max_delay": 10.0,
        })
        self.assertEqual(kwargs["timeout"], 5)
        self.assertEqual(kwargs["max_retries"], 0)
        self.assertEqual(kwargs["retry_backoff"], 0.25)
        self.assertEqual(kwargs["retry_max_delay"], 10.0)


# ===========================================================================
# load_config テスト
# ===========================================================================


class TestLoadConfig(unittest.TestCase):
    def setUp(self):
        self._dir = tempfile.TemporaryDirectory()
        self.addCleanup(self._dir.cleanup)
        self.dir = pathlib.Path(self._dir.name)

    def _write(self, name, text):
        path = self.dir / name
        path.write_text(text, encoding="utf-8")
        return str(path)

    def test_missing_file_raises_config_error(self):
        with self.assertRaises(sut.ConfigError) as ctx:
            sut.load_config(str(self.dir / "存在しない.yaml"))
        self.assertIn("設定ファイルが見つかりません", str(ctx.exception))

    def test_reads_yaml_mapping(self):
        path = self._write(
            "c.yaml",
            'backlog:\n  space_host: "t.backlog.com"\n  api_key: "K"\n'
            'clone:\n  source_issue_key: "P-1"\n  summary_template: "件名"\n',
        )
        cfg = sut.load_config(path)
        self.assertEqual(cfg["backlog"]["space_host"], "t.backlog.com")
        self.assertEqual(cfg["clone"]["summary_template"], "件名")

    def test_empty_file_returns_none_and_validate_rejects_it(self):
        """空ファイルは None になる。validate_config が弾くことまで確認する。"""
        cfg = sut.load_config(self._write("empty.yaml", ""))
        self.assertIsNone(cfg)
        with self.assertRaises(sut.ConfigError):
            sut.validate_config(cfg)


# ===========================================================================
# build_parser テスト
# ===========================================================================


class TestBuildParser(unittest.TestCase):
    def parse(self, argv):
        return sut.build_parser().parse_args(argv)

    def test_defaults(self):
        args = self.parse([])
        self.assertFalse(args.execute)
        self.assertFalse(args.yes)
        self.assertFalse(args.detailed_exit_code)
        self.assertFalse(args.debug)
        self.assertIsNone(args.date)
        self.assertTrue(args.config.endswith("config.yaml"))

    def test_all_flags(self):
        args = self.parse(["--execute", "--yes", "--detailed-exit-code", "--debug"])
        self.assertTrue(args.execute)
        self.assertTrue(args.yes)
        self.assertTrue(args.detailed_exit_code)
        self.assertTrue(args.debug)

    def test_short_yes(self):
        self.assertTrue(self.parse(["-y"]).yes)

    def test_config_and_date(self):
        args = self.parse(["--config", "my.yaml", "--date", "20260401"])
        self.assertEqual(args.config, "my.yaml")
        self.assertEqual(args.date, "20260401")

    def test_unknown_option_exits(self):
        with patch("sys.stderr", new_callable=StringIO):
            with self.assertRaises(SystemExit):
                self.parse(["--存在しない"])


# ===========================================================================
# main() テスト（終了コードへの変換）
# ===========================================================================


class TestMain(unittest.TestCase):
    """main() が実行結果・例外を終了コードに変換することを検証。"""

    def _main(self, argv=(), *, outcome=sut.OUTCOME_NO_CHANGE, error=None,
              load_error=None):
        out, err = StringIO(), StringIO()
        load = (
            patch("backlog_issue_cloner.load_config", side_effect=load_error)
            if load_error
            else patch("backlog_issue_cloner.load_config", return_value=_make_config())
        )
        with patch("sys.argv", ["backlog_issue_cloner.py", *argv]), load, \
             patch("backlog_issue_cloner.validate_config"), \
             patch("backlog_issue_cloner.run",
                   side_effect=error or (lambda *a, **kw: outcome)), \
             patch("sys.stdout", out), patch("sys.stderr", err):
            with self.assertRaises(SystemExit) as ctx:
                sut.main()
        return ctx.exception.code, out.getvalue(), err.getvalue()

    # --- 正常終了 ---

    def test_success_returns_zero_without_detailed_flag(self):
        for outcome in (sut.OUTCOME_NO_CHANGE, sut.OUTCOME_CREATED, sut.OUTCOME_UPDATED):
            code, _, _ = self._main(["--execute", "--yes"], outcome=outcome)
            self.assertEqual(code, 0, f"outcome={outcome}")

    def test_detailed_exit_code_distinguishes_outcomes(self):
        expected = {
            sut.OUTCOME_NO_CHANGE: 0,
            sut.OUTCOME_CREATED: 10,
            sut.OUTCOME_UPDATED: 11,
        }
        for outcome, want in expected.items():
            code, _, _ = self._main(
                ["--execute", "--yes", "--detailed-exit-code"], outcome=outcome
            )
            self.assertEqual(code, want, f"outcome={outcome}")

    def test_skipped_returns_twenty(self):
        code, _, _ = self._main(["--execute"], outcome=sut.OUTCOME_SKIPPED)
        self.assertEqual(code, 20)

    def test_skipped_returns_twenty_with_detailed_flag(self):
        code, _, _ = self._main(
            ["--execute", "--detailed-exit-code"], outcome=sut.OUTCOME_SKIPPED
        )
        self.assertEqual(code, 20)

    # --- エラー ---

    def test_config_error_from_load_returns_two(self):
        code, _, err = self._main(load_error=sut.ConfigError("設定ファイルが見つかりません"))
        self.assertEqual(code, 2)
        self.assertIn("設定ファイルが見つかりません", err)

    def test_config_error_from_run_returns_two(self):
        code, _, err = self._main(
            ["--execute"], error=sut.ConfigError("コピー元課題が見つかりません。")
        )
        self.assertEqual(code, 2)
        self.assertIn("コピー元課題が見つかりません。", err)

    def test_backlog_error_returns_three_with_hint(self):
        code, _, err = self._main(
            ["--execute"],
            error=sut.BacklogError("認証に失敗", status=401, hint="api_key を確認してください。"),
        )
        self.assertEqual(code, 3)
        self.assertIn("認証に失敗", err)
        self.assertIn("api_key を確認してください。", err)

    def test_backlog_error_without_hint(self):
        code, _, err = self._main(["--execute"], error=sut.BacklogError("不明なエラー"))
        self.assertEqual(code, 3)
        self.assertIn("不明なエラー", err)

    def test_no_change_error_is_a_backlog_error(self):
        """BacklogNoChangeError も BacklogError として捕捉される。"""
        code, _, _ = self._main(["--execute"], error=sut.BacklogNoChangeError("変更なし"))
        self.assertEqual(code, 3)

    # --- バナー表示 ---

    def test_banner_shows_dry_run_by_default(self):
        _, out, _ = self._main([])
        self.assertIn("DRY RUN", out)
        self.assertIn("test.backlog.com", out)

    def test_banner_shows_execute_with_flag(self):
        _, out, _ = self._main(["--execute", "--yes"])
        self.assertIn("EXECUTE", out)
        self.assertNotIn("DRY RUN", out)

    # --- 引数の受け渡し ---

    def test_args_are_passed_to_run(self):
        with patch("sys.argv", ["prog", "--execute", "-y", "--date", "20260401"]), \
             patch("backlog_issue_cloner.load_config", return_value=_make_config()), \
             patch("backlog_issue_cloner.validate_config"), \
             patch("backlog_issue_cloner.run",
                   return_value=sut.OUTCOME_NO_CHANGE) as mock_run, \
             patch("sys.stdout", new_callable=StringIO):
            with self.assertRaises(SystemExit):
                sut.main()
        args = mock_run.call_args[0][0]
        self.assertTrue(args.execute)
        self.assertTrue(args.yes)
        self.assertEqual(args.date, "20260401")


if __name__ == "__main__":
    unittest.main(verbosity=2)
