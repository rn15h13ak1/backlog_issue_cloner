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

    def test_exclude_id_skips_that_issue(self):
        """summary_template 省略時にコピー元自身がヒットしないことを保証する。"""
        issues = [
            {"id": 1, "issueKey": "PROJ-1", "summary": "テンプレート課題"},
            {"id": 2, "issueKey": "PROJ-9", "summary": "テンプレート課題"},
        ]
        client = self._make_client(issues)
        result = sut.find_existing_by_summary(
            client, 10, "テンプレート課題", exclude_id=1
        )
        self.assertEqual(result["issueKey"], "PROJ-9")

    def test_exclude_id_returns_none_when_only_self_matches(self):
        issues = [{"id": 1, "issueKey": "PROJ-1", "summary": "テンプレート課題"}]
        client = self._make_client(issues)
        result = sut.find_existing_by_summary(
            client, 10, "テンプレート課題", exclude_id=1
        )
        self.assertIsNone(result)

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

    def test_get_child_issues_filters_by_parent(self):
        client = self._client()
        children = [{"issueKey": "P-2", "summary": "子"}]
        with patch.object(client, "_get", side_effect=[children]) as mock_get:
            result = list(client.get_child_issues(1001))
        self.assertEqual(result, children)
        params = mock_get.call_args[0][1]
        self.assertEqual(params["parentIssueId"], [1001])
        # 完了済みの子課題を再作成しないよう状態では絞り込まない
        self.assertNotIn("statusId", params)
        self.assertNotIn("projectId", params)

    def test_get_child_issues_paginates(self):
        client = self._client()
        pages = [self._page(100, "子"), [{"issueKey": "P-x", "summary": "子"}]]
        with patch.object(client, "_get", side_effect=pages) as mock_get, \
             patch("backlog_issue_cloner.time.sleep"):
            result = list(client.get_child_issues(1001))
        self.assertEqual(len(result), 101)
        self.assertEqual(mock_get.call_args_list[1][0][1]["offset"], 100)


# ===========================================================================
# パラメータ展開テスト
# ===========================================================================


class TestCustomFieldParams(unittest.TestCase):
    """必須のカスタム属性があるプロジェクトでは、渡さないと作成が 400 になる。"""

    def _field(self, id_, type_id, value, **extra):
        return {"id": id_, "fieldTypeId": type_id, "name": "属性",
                "value": value, **extra}

    def test_no_custom_fields(self):
        self.assertEqual(sut.custom_field_params({}), {})
        self.assertEqual(sut.custom_field_params({"customFields": None}), {})
        self.assertEqual(sut.custom_field_params({"customFields": []}), {})

    def test_text_and_number(self):
        issue = {"customFields": [self._field(1, 1, "文字列"), self._field(2, 3, 42)]}
        self.assertEqual(
            sut.custom_field_params(issue),
            {"customField_1": "文字列", "customField_2": 42},
        )

    def test_date_is_trimmed_to_day(self):
        """読み取りは ISO 形式だが、書き込みは日付のみを受け付ける。"""
        issue = {"customFields": [self._field(3, 4, "2026-09-09T00:00:00Z")]}
        self.assertEqual(sut.custom_field_params(issue), {"customField_3": "2026-09-09"})

    def test_single_select_sends_id(self):
        issue = {"customFields": [self._field(4, 5, {"id": 77, "name": "選択肢"})]}
        self.assertEqual(sut.custom_field_params(issue), {"customField_4": 77})

    def test_radio_sends_id(self):
        issue = {"customFields": [self._field(5, 8, {"id": 88, "name": "選択肢"})]}
        self.assertEqual(sut.custom_field_params(issue), {"customField_5": 88})

    def test_checkbox_sends_id_list(self):
        """「作業完了チェック」のようなチェックボックスは ID の配列で送る。"""
        issue = {"customFields": [
            self._field(6, 7, [{"id": 1, "name": "A"}, {"id": 2, "name": "B"}])
        ]}
        self.assertEqual(sut.custom_field_params(issue), {"customField_6": [1, 2]})

    def test_multi_select_sends_id_list(self):
        issue = {"customFields": [self._field(7, 6, [{"id": 9, "name": "X"}])]}
        self.assertEqual(sut.custom_field_params(issue), {"customField_7": [9]})

    def test_other_value_is_included(self):
        issue = {"customFields": [
            self._field(8, 5, {"id": 3, "name": "その他"}, otherValue="自由記入")
        ]}
        self.assertEqual(
            sut.custom_field_params(issue),
            {"customField_8": 3, "customField_8_otherValue": "自由記入"},
        )

    def test_empty_values_are_skipped(self):
        issue = {"customFields": [
            self._field(1, 1, None), self._field(2, 7, []),
            self._field(3, 5, None), {"fieldTypeId": 1, "value": "id が無い"},
        ]}
        self.assertEqual(sut.custom_field_params(issue), {})

    def test_malformed_select_is_skipped(self):
        issue = {"customFields": [
            self._field(1, 5, "オブジェクトでない"),
            self._field(2, 7, ["オブジェクトでない"]),
        ]}
        self.assertEqual(sut.custom_field_params(issue), {})

    def test_list_values_are_expanded_for_the_api(self):
        """チェックボックスは customField_6[]=1&customField_6[]=2 の形で送られる。"""
        params = sut.custom_field_params(
            {"customFields": [self._field(6, 7, [{"id": 1}, {"id": 2}])]}
        )
        self.assertEqual(
            sut._flatten_params(params),
            [("customField_6[]", "1"), ("customField_6[]", "2")],
        )


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

    def test_exact_name_match(self):
        id_, name = sut.resolve_issue_type_id(self.TYPES, "バグ")
        self.assertEqual(id_, 2)
        self.assertEqual(name, "バグ")

    def test_fallback_to_first_when_not_found(self):
        with patch("sys.stderr", new_callable=StringIO):
            id_, name = sut.resolve_issue_type_id(self.TYPES, "存在しない種別")
        self.assertEqual(id_, 1)
        self.assertEqual(name, "タスク")

    def test_none_returns_first(self):
        id_, name = sut.resolve_issue_type_id(self.TYPES, None)
        self.assertEqual(id_, 1)
        self.assertEqual(name, "タスク")

    def test_fetch_raises_when_empty(self):
        client = MagicMock()
        client.get_issue_types.return_value = []
        with self.assertRaises(sut.ConfigError):
            sut.fetch_issue_types(client, "PROJ")

    def test_fetch_returns_types(self):
        client = MagicMock()
        client.get_issue_types.return_value = self.TYPES
        self.assertEqual(sut.fetch_issue_types(client, "PROJ"), self.TYPES)


# ===========================================================================
# resolve_priority_id テスト
# ===========================================================================


class TestResolvePriorityId(unittest.TestCase):
    PRIORITIES = [
        {"id": 2, "name": "高"},
        {"id": 3, "name": "中"},
        {"id": 4, "name": "低"},
    ]

    def test_exact_name_match(self):
        id_, name = sut.resolve_priority_id(self.PRIORITIES, "高")
        self.assertEqual(id_, 2)
        self.assertEqual(name, "高")

    def test_fallback_to_chuu_when_none(self):
        id_, name = sut.resolve_priority_id(self.PRIORITIES, None)
        self.assertEqual(id_, 3)
        self.assertEqual(name, "中")

    def test_warns_and_falls_back_to_chuu_when_not_found(self):
        """指定した優先度が見つからない場合は警告して「中」を使う。"""
        with patch("sys.stderr", new_callable=StringIO) as err:
            id_, name = sut.resolve_priority_id(self.PRIORITIES, "存在しない優先度")
        self.assertEqual(id_, 3)
        self.assertEqual(name, "中")
        message = err.getvalue()
        self.assertIn("存在しない優先度", message)
        self.assertIn("高", message)  # 利用可能な値を案内する

    def test_falls_back_to_first_when_not_found_and_no_chuu(self):
        priorities = [{"id": 2, "name": "高"}, {"id": 4, "name": "低"}]
        with patch("sys.stderr", new_callable=StringIO):
            id_, name = sut.resolve_priority_id(priorities, "存在しない優先度")
        self.assertEqual(id_, 2)
        self.assertEqual(name, "高")

    def test_fallback_to_first_when_chuu_not_found(self):
        priorities = [{"id": 2, "name": "高"}, {"id": 4, "name": "低"}]
        id_, name = sut.resolve_priority_id(priorities, None)
        self.assertEqual(id_, 2)
        self.assertEqual(name, "高")

    def test_fetch_raises_when_empty(self):
        client = MagicMock()
        client.get_priorities.return_value = []
        with self.assertRaises(sut.ConfigError):
            sut.fetch_priorities(client)


# ===========================================================================
# 子課題の複製計画テスト
# ===========================================================================


class TestBuildSummary(unittest.TestCase):
    def test_default_template_keeps_source_summary(self):
        self.assertEqual(
            sut.build_summary("{SOURCE_SUMMARY}", "手順1 バックアップ", "20260828"),
            "手順1 バックアップ",
        )

    def test_date_placeholder(self):
        self.assertEqual(
            sut.build_summary("{YYYYMMDD} {SOURCE_SUMMARY}", "点検", "20260828"),
            "20260828 点検",
        )

    def test_static_template(self):
        self.assertEqual(sut.build_summary("固定", "元", "20260828"), "固定")


class TestBuildChildPlans(unittest.TestCase):
    OPTS = dict(template="{SOURCE_SUMMARY}", date_str="20260828", match_mode="substring")

    def test_all_created_when_no_existing_children(self):
        sources = [_child(1, "P-2", "子A"), _child(2, "P-3", "子B")]
        plans = sut.build_child_plans(sources, [], **self.OPTS)
        self.assertEqual([p.action for p in plans],
                         [sut.OUTCOME_CREATED, sut.OUTCOME_CREATED])
        self.assertEqual([p.summary for p in plans], ["子A", "子B"])
        self.assertTrue(all(p.existing is None for p in plans))

    def test_no_change_when_description_matches(self):
        sources = [_child(1, "P-2", "子A", "同じ本文")]
        existing = [_child(9, "Q-2", "子A", "同じ本文")]
        plans = sut.build_child_plans(sources, existing, **self.OPTS)
        self.assertEqual(plans[0].action, sut.OUTCOME_NO_CHANGE)
        self.assertEqual(plans[0].existing["issueKey"], "Q-2")

    def test_updated_when_description_differs(self):
        sources = [_child(1, "P-2", "子A", "新しい本文")]
        existing = [_child(9, "Q-2", "子A", "古い本文")]
        plans = sut.build_child_plans(sources, existing, **self.OPTS)
        self.assertEqual(plans[0].action, sut.OUTCOME_UPDATED)

    def test_mixed(self):
        sources = [
            _child(1, "P-2", "子A", "同じ"),
            _child(2, "P-3", "子B", "新"),
            _child(3, "P-4", "子C", "本文"),
        ]
        existing = [_child(9, "Q-2", "子A", "同じ"), _child(8, "Q-3", "子B", "旧")]
        plans = sut.build_child_plans(sources, existing, **self.OPTS)
        self.assertEqual(
            [p.action for p in plans],
            [sut.OUTCOME_NO_CHANGE, sut.OUTCOME_UPDATED, sut.OUTCOME_CREATED],
        )

    def test_each_existing_child_matched_only_once(self):
        """同名の子課題が複数あっても既存 1 件を重複して割り当てない。"""
        sources = [_child(1, "P-2", "子A", "本文"), _child(2, "P-3", "子A", "本文")]
        existing = [_child(9, "Q-2", "子A", "本文")]
        plans = sut.build_child_plans(sources, existing, **self.OPTS)
        self.assertEqual(plans[0].action, sut.OUTCOME_NO_CHANGE)
        self.assertEqual(plans[1].action, sut.OUTCOME_CREATED)

    def test_exact_match_mode(self):
        sources = [_child(1, "P-2", "子A")]
        existing = [_child(9, "Q-2", "子A（別）")]
        plans = sut.build_child_plans(
            sources, existing, **{**self.OPTS, "match_mode": "exact"}
        )
        self.assertEqual(plans[0].action, sut.OUTCOME_CREATED)

    def test_empty_sources(self):
        self.assertEqual(sut.build_child_plans([], [], **self.OPTS), [])


class TestFindUnmatchedChildren(unittest.TestCase):
    OPTS = dict(template="{SOURCE_SUMMARY}", date_str="20260828", match_mode="exact")

    def test_renamed_existing_child_is_unmatched(self):
        sources = [_child(1, "P-2", "手順2 検証")]
        existing = [_child(9, "D-7", "検証手順")]
        plans = sut.build_child_plans(sources, existing, **self.OPTS)
        unmatched = sut.find_unmatched_children(existing, plans)
        self.assertEqual([c["issueKey"] for c in unmatched], ["D-7"])

    def test_all_matched_returns_empty(self):
        sources = [_child(1, "P-2", "手順1"), _child(2, "P-3", "手順2")]
        existing = [_child(9, "D-6", "手順1"), _child(8, "D-7", "手順2")]
        plans = sut.build_child_plans(sources, existing, **self.OPTS)
        self.assertEqual(sut.find_unmatched_children(existing, plans), [])

    def test_extra_existing_child_is_reported(self):
        sources = [_child(1, "P-2", "手順1")]
        existing = [_child(9, "D-6", "手順1"), _child(8, "D-9", "現地対応メモ")]
        plans = sut.build_child_plans(sources, existing, **self.OPTS)
        unmatched = sut.find_unmatched_children(existing, plans)
        self.assertEqual([c["issueKey"] for c in unmatched], ["D-9"])

    def test_no_existing_children(self):
        plans = sut.build_child_plans([_child(1, "P-2", "手順1")], [], **self.OPTS)
        self.assertEqual(sut.find_unmatched_children([], plans), [])


class TestResolveChildIssueType(unittest.TestCase):
    TYPES = [{"id": 1, "name": "タスク"}, {"id": 2, "name": "バグ"}]

    def test_matches_by_name(self):
        source = _child(1, "P-2", "子", type_name="バグ")
        self.assertEqual(
            sut.resolve_child_issue_type(self.TYPES, source, (1, "タスク")), (2, "バグ")
        )

    def test_falls_back_when_name_absent_in_target(self):
        source = _child(1, "P-2", "子", type_name="複製先に無い種別")
        self.assertEqual(
            sut.resolve_child_issue_type(self.TYPES, source, (1, "タスク")), (1, "タスク")
        )

    def test_falls_back_when_no_issue_type(self):
        self.assertEqual(
            sut.resolve_child_issue_type(self.TYPES, {"summary": "子"}, (1, "タスク")),
            (1, "タスク"),
        )


class TestAggregateOutcome(unittest.TestCase):
    def test_skipped_wins(self):
        actions = [sut.OUTCOME_CREATED, sut.OUTCOME_SKIPPED, sut.OUTCOME_UPDATED]
        self.assertEqual(sut.aggregate_outcome(actions), sut.OUTCOME_SKIPPED)

    def test_created_beats_updated(self):
        actions = [sut.OUTCOME_NO_CHANGE, sut.OUTCOME_UPDATED, sut.OUTCOME_CREATED]
        self.assertEqual(sut.aggregate_outcome(actions), sut.OUTCOME_CREATED)

    def test_updated_beats_no_change(self):
        self.assertEqual(
            sut.aggregate_outcome([sut.OUTCOME_NO_CHANGE, sut.OUTCOME_UPDATED]),
            sut.OUTCOME_UPDATED,
        )

    def test_all_no_change(self):
        self.assertEqual(
            sut.aggregate_outcome([sut.OUTCOME_NO_CHANGE]), sut.OUTCOME_NO_CHANGE
        )


# ===========================================================================
# 確認プロンプトテスト
# ===========================================================================


class TestConfirm(unittest.TestCase):
    def _plan(self, action, summary="子の件名"):
        return sut.ChildPlan(source={}, summary=summary, action=action)

    def test_assume_yes_skips_input(self):
        with patch("sys.stdout", new_callable=StringIO), \
             patch("builtins.input", side_effect=AssertionError("input が呼ばれた")):
            self.assertTrue(
                sut.confirm_plan(sut.OUTCOME_CREATED, [], assume_yes=True)
            )

    def test_non_interactive_without_yes_returns_false(self):
        """非対話環境で --yes なしなら input を呼ばずに False。"""
        with patch("sys.stdout", new_callable=StringIO), \
             patch("sys.stderr", new_callable=StringIO) as err, \
             patch("sys.stdin.isatty", return_value=False), \
             patch("builtins.input", side_effect=AssertionError("input が呼ばれた")):
            result = sut.confirm_plan(sut.OUTCOME_CREATED, [])
        self.assertFalse(result)
        self.assertIn("--yes", err.getvalue())

    def test_interactive_yes(self):
        with patch("sys.stdout", new_callable=StringIO), tty(), \
             patch("builtins.input", return_value="y"):
            self.assertTrue(sut.confirm_plan(sut.OUTCOME_UPDATED, []))

    def test_interactive_no(self):
        with patch("sys.stdout", new_callable=StringIO), tty(), \
             patch("builtins.input", return_value="n"):
            self.assertFalse(sut.confirm_plan(sut.OUTCOME_UPDATED, []))

    def test_eof_treated_as_no(self):
        with patch("sys.stdout", new_callable=StringIO), tty(), \
             patch("builtins.input", side_effect=EOFError):
            self.assertFalse(sut.confirm_plan(sut.OUTCOME_UPDATED, []))

    def test_no_confirmation_when_nothing_to_do(self):
        """作成も更新も無い場合は確認せず True。"""
        with patch("builtins.input", side_effect=AssertionError("input が呼ばれた")):
            self.assertTrue(
                sut.confirm_plan(
                    sut.OUTCOME_NO_CHANGE, [self._plan(sut.OUTCOME_NO_CHANGE)]
                )
            )

    def test_prompt_counts_parent_and_children(self):
        captured = {}
        with patch("sys.stdout", new_callable=StringIO), tty(), \
             patch("builtins.input", side_effect=lambda p: captured.setdefault("p", p) and "y"):
            sut.confirm_plan(
                sut.OUTCOME_CREATED,
                [
                    self._plan(sut.OUTCOME_CREATED),
                    self._plan(sut.OUTCOME_UPDATED),
                    self._plan(sut.OUTCOME_NO_CHANGE),
                ],
            )
        self.assertIn("新規作成 2 件", captured["p"])
        self.assertIn("本文更新 1 件", captured["p"])

    def _print_plan(self, child_plans, unmatched):
        out = StringIO()
        with patch("sys.stdout", out):
            sut.print_plan(
                sut.OUTCOME_NO_CHANGE, "親", {"issueKey": "D-5"}, child_plans, unmatched
            )
        return out.getvalue()

    def test_warns_when_creating_and_unmatched_exists(self):
        text = self._print_plan(
            [self._plan(sut.OUTCOME_CREATED, "手順2 検証")],
            [{"issueKey": "D-7", "summary": "検証手順"}],
        )
        self.assertIn("警告", text)
        self.assertIn("D-7: 検証手順", text)
        self.assertIn("重複", text)

    def test_no_warning_when_nothing_is_created(self):
        """作成が無ければ重複しないので、未照合があっても黙っている。"""
        text = self._print_plan(
            [self._plan(sut.OUTCOME_UPDATED, "手順1")],
            [{"issueKey": "D-9", "summary": "現地対応メモ"}],
        )
        self.assertNotIn("警告", text)

    def test_no_warning_when_nothing_unmatched(self):
        text = self._print_plan([self._plan(sut.OUTCOME_CREATED, "手順2")], [])
        self.assertNotIn("警告", text)

    def test_labels_are_aligned_regardless_of_length(self):
        """新規作成(4文字)と本文を更新(5文字)で件名の開始位置がずれない。"""
        text = self._print_plan(
            [self._plan(sut.OUTCOME_CREATED, "子A"),
             self._plan(sut.OUTCOME_UPDATED, "子B"),
             self._plan(sut.OUTCOME_NO_CHANGE, "子C")],
            [],
        )
        starts = {
            sut.display_width(line.split("子")[0])
            for line in text.splitlines() if "[子]" in line
        }
        self.assertEqual(len(starts), 1, f"件名の開始位置がずれている: {starts}")

    def test_print_plan_lists_parent_and_children(self):
        out = StringIO()
        with patch("sys.stdout", out):
            sut.print_plan(
                sut.OUTCOME_UPDATED,
                "親の件名",
                {"issueKey": "PROJ-99"},
                [self._plan(sut.OUTCOME_CREATED, "子A")],
            )
        text = out.getvalue()
        self.assertIn("[親]", text)
        self.assertIn("PROJ-99", text)
        self.assertIn("[子]", text)
        self.assertIn("子A", text)


# ===========================================================================
# 終了コードテスト
# ===========================================================================


class TestDisplayWidth(unittest.TestCase):
    def test_ascii_counts_one_each(self):
        self.assertEqual(sut.display_width("abc12"), 5)

    def test_full_width_counts_two_each(self):
        self.assertEqual(sut.display_width("新規作成"), 8)
        self.assertEqual(sut.display_width("本文を更新"), 10)

    def test_mixed(self):
        # 半角 7 文字 + 全角 2 文字
        self.assertEqual(sut.display_width("PROJ-1 課題"), 11)

    def test_pad_fills_to_display_width(self):
        self.assertEqual(sut.display_width(sut.pad("新規作成", 12)), 12)
        self.assertEqual(sut.display_width(sut.pad("本文を更新", 12)), 12)

    def test_pad_does_not_truncate(self):
        self.assertEqual(sut.pad("長すぎるラベル", 4), "長すぎるラベル")


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
    issue_type=None,
    priority=None,
    match_mode=None,
    include_closed=None,
    target_issue_key=None,
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
    if issue_type:
        cfg["clone"]["issue_type"] = issue_type
    if priority:
        cfg["clone"]["priority"] = priority
    if match_mode:
        cfg["clone"]["match_mode"] = match_mode
    if include_closed is not None:
        cfg["clone"]["include_closed"] = include_closed
    if summary_template is None:
        del cfg["clone"]["summary_template"]
    if target_issue_key:
        cfg["clone"]["target_issue_key"] = target_issue_key
    return cfg


SOURCE_ISSUE = {
    "id": 1001,
    "issueKey": "PROJ-1",
    "summary": "テンプレート課題",
    "description": "本文テキスト",
    "projectId": 10,
}

PROJECT = {"id": 10, "projectKey": "PROJ"}
ISSUE_TYPES = [{"id": 1, "name": "タスク"}, {"id": 2, "name": "バグ"}]
PRIORITIES = [{"id": 2, "name": "高"}, {"id": 3, "name": "中"}]


def _child(id_, key, summary, description="子の本文", type_name="タスク", priority_id=3):
    return {
        "id": id_,
        "issueKey": key,
        "summary": summary,
        "description": description,
        "issueType": {"id": 1, "name": type_name},
        "priority": {"id": priority_id, "name": "中"},
    }


def _mock_client(existing_issue=None, source_children=None, existing_children=None):
    mock_client = MagicMock()
    mock_client.get_issue.return_value = SOURCE_ISSUE
    mock_client.get_project.return_value = PROJECT
    mock_client.get_issue_types.return_value = ISSUE_TYPES
    mock_client.get_priorities.return_value = PRIORITIES
    mock_client.create_issue.return_value = {
        "id": 2001, "issueKey": "PROJ-100", "summary": "【定期】20260828 タスク"
    }
    mock_client.update_issue.return_value = {
        "issueKey": "PROJ-99", "summary": "【定期】20260828 タスク"
    }
    mock_client.search_issues_by_keyword.return_value = (
        [existing_issue] if existing_issue else []
    )
    # コピー元の子課題 → 既存の親に紐づく子課題、の順で呼ばれる
    mock_client.get_child_issues.side_effect = (
        lambda parent_id: list(
            source_children or [] if parent_id == SOURCE_ISSUE["id"]
            else existing_children or []
        )
    )
    return patch("backlog_issue_cloner.BacklogClient", return_value=mock_client), mock_client


EXISTING_SAME = {
    "id": 1099,
    "issueKey": "PROJ-99",
    "summary": "【定期】20260828 タスク",
    "description": "本文テキスト",
}
EXISTING_DIFF = {
    "id": 1099,
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

    # --- カスタム属性の引き継ぎ ---

    CUSTOM_FIELDS = [
        {"id": 11, "fieldTypeId": 7, "name": "作業完了チェック",
         "value": [{"id": 1, "name": "済"}]},
        {"id": 12, "fieldTypeId": 1, "name": "備考", "value": "メモ"},
    ]

    def test_custom_fields_are_copied_on_create(self):
        """必須のカスタム属性があるプロジェクトでも作成できるようにする。"""
        patcher, mock_client = _mock_client()
        mock_client.get_issue.return_value = {
            **SOURCE_ISSUE, "customFields": self.CUSTOM_FIELDS
        }
        with patcher, patch("sys.stdout", new_callable=StringIO), tty(), \
             patch("builtins.input", return_value="y"):
            sut.run(_make_args(execute=True, date="20260828"), _make_config())
        params = mock_client.create_issue.call_args[0][0]
        self.assertEqual(params["customField_11"], [1])
        self.assertEqual(params["customField_12"], "メモ")

    def test_custom_fields_can_be_disabled(self):
        cfg = _make_config()
        cfg["clone"]["copy_custom_fields"] = False
        patcher, mock_client = _mock_client()
        mock_client.get_issue.return_value = {
            **SOURCE_ISSUE, "customFields": self.CUSTOM_FIELDS
        }
        with patcher, patch("sys.stdout", new_callable=StringIO), tty(), \
             patch("builtins.input", return_value="y"):
            sut.run(_make_args(execute=True, date="20260828"), cfg)
        params = mock_client.create_issue.call_args[0][0]
        self.assertNotIn("customField_11", params)

    def test_child_custom_fields_are_copied(self):
        child = _child(101, "PROJ-2", "手順1", "本文1")
        child["customFields"] = self.CUSTOM_FIELDS
        patcher, mock_client = _mock_client(source_children=[child])
        with patcher, patch("sys.stdout", new_callable=StringIO), tty(), \
             patch("builtins.input", return_value="y"):
            sut.run(_make_args(execute=True, date="20260828"), _make_config())
        child_params = mock_client.create_issue.call_args_list[1][0][0]
        self.assertEqual(child_params["customField_11"], [1])

    def test_child_is_refetched_when_custom_fields_absent(self):
        """課題一覧のレスポンスにカスタム属性が無い場合は取得し直す。"""
        child = _child(101, "PROJ-2", "手順1", "本文1")  # customFields を持たない
        patcher, mock_client = _mock_client(source_children=[child])
        full_child = {**child, "customFields": self.CUSTOM_FIELDS}
        mock_client.get_issue.side_effect = lambda key: (
            SOURCE_ISSUE if key == "PROJ-1" else full_child
        )
        with patcher, patch("sys.stdout", new_callable=StringIO), tty(), \
             patch("builtins.input", return_value="y"):
            sut.run(_make_args(execute=True, date="20260828"), _make_config())
        mock_client.get_issue.assert_any_call("PROJ-2")
        child_params = mock_client.create_issue.call_args_list[1][0][0]
        self.assertEqual(child_params["customField_11"], [1])

    def test_child_is_not_refetched_when_custom_fields_present(self):
        child = _child(101, "PROJ-2", "手順1", "本文1")
        child["customFields"] = []
        patcher, mock_client = _mock_client(source_children=[child])
        with patcher, patch("sys.stdout", new_callable=StringIO), tty(), \
             patch("builtins.input", return_value="y"):
            sut.run(_make_args(execute=True, date="20260828"), _make_config())
        self.assertEqual(
            [c[0][0] for c in mock_client.get_issue.call_args_list], ["PROJ-1"]
        )

    def test_always_creates_in_source_project(self):
        """複製先は常にコピー元と同じプロジェクト。"""
        patcher, mock_client = _mock_client()
        with patcher, patch("sys.stdout", new_callable=StringIO), tty(), \
             patch("builtins.input", return_value="y"):
            sut.run(_make_args(execute=True, date="20260828"), _make_config())
        self.assertEqual(
            mock_client.create_issue.call_args[0][0]["projectId"],
            SOURCE_ISSUE["projectId"],
        )

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

    def test_sample_looking_source_issue_key_is_accepted(self):
        """PROJ-123 は実在しうるキーなので、サンプル値として弾かない。"""
        cfg = self._base_config()
        cfg["clone"]["source_issue_key"] = "PROJ-123"
        sut.validate_config(cfg)

    def test_empty_source_issue_key_raises(self):
        cfg = self._base_config()
        cfg["clone"]["source_issue_key"] = ""
        with self.assertRaises(sut.ConfigError) as ctx:
            sut.validate_config(cfg)
        self.assertIn("--source-issue-key", str(ctx.exception))

    def test_empty_summary_template_raises(self):
        cfg = self._base_config()
        cfg["clone"]["summary_template"] = ""
        with self.assertRaises(sut.ConfigError):
            sut.validate_config(cfg)

    def test_omitted_summary_template_passes(self):
        """省略時はコピー元の件名をそのまま使うため必須ではない。"""
        cfg = self._base_config()
        del cfg["clone"]["summary_template"]
        sut.validate_config(cfg)

    # --- target_issue_key ---

    def test_target_issue_key_passes(self):
        cfg = self._base_config()
        cfg["clone"]["target_issue_key"] = "DEST-5"
        sut.validate_config(cfg)

    def test_empty_target_issue_key_raises(self):
        cfg = self._base_config()
        cfg["clone"]["target_issue_key"] = ""
        with self.assertRaises(sut.ConfigError):
            sut.validate_config(cfg)

    def test_target_project_key_is_rejected(self):
        """廃止項目。黙って無視すると別プロジェクトに作られると誤解されるため止める。"""
        cfg = self._base_config()
        cfg["clone"]["target_project_key"] = "DEST"
        with self.assertRaises(sut.ConfigError) as ctx:
            sut.validate_config(cfg)
        self.assertIn("廃止", str(ctx.exception))

    def test_target_project_key_rejected_even_with_target_issue_key(self):
        cfg = self._base_config()
        cfg["clone"]["target_issue_key"] = "PROJ-5"
        cfg["clone"]["target_project_key"] = "DEST"
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

    # --- 子課題の設定 ---

    def test_string_for_include_children_raises(self):
        cfg = self._base_config()
        cfg["clone"]["include_children"] = "true"
        with self.assertRaises(sut.ConfigError):
            sut.validate_config(cfg)

    def test_bool_for_include_children_passes(self):
        for value in (True, False):
            cfg = self._base_config()
            cfg["clone"]["include_children"] = value
            sut.validate_config(cfg)

    def test_empty_child_summary_template_raises(self):
        cfg = self._base_config()
        cfg["clone"]["child_summary_template"] = ""
        with self.assertRaises(sut.ConfigError):
            sut.validate_config(cfg)

    def test_child_summary_template_passes(self):
        cfg = self._base_config()
        cfg["clone"]["child_summary_template"] = "{YYYYMMDD} {SOURCE_SUMMARY}"
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


class TestRunWithChildren(unittest.TestCase):
    """親課題と子課題をまとめて複製する経路を検証。"""

    SOURCE_CHILDREN = [
        _child(101, "PROJ-2", "手順1 バックアップ", "本文1"),
        _child(102, "PROJ-3", "手順2 検証", "本文2", type_name="バグ"),
    ]

    def _run(self, *, existing=None, source_children=None, existing_children=None,
             config=None, execute=True, answer="y"):
        patcher, mc = _mock_client(
            existing_issue=existing,
            source_children=source_children,
            existing_children=existing_children,
        )
        out = StringIO()
        with patcher, patch("sys.stdout", out), patch("sys.stderr", new_callable=StringIO), \
             tty(), patch("builtins.input", return_value=answer):
            outcome = sut.run(
                _make_args(execute=execute, date="20260828"), config or _make_config()
            )
        return outcome, mc, out.getvalue()

    def test_creates_parent_and_children(self):
        outcome, mc, _ = self._run(source_children=self.SOURCE_CHILDREN)
        self.assertEqual(outcome, sut.OUTCOME_CREATED)
        self.assertEqual(mc.create_issue.call_count, 3)  # 親 1 + 子 2

        parent_params = mc.create_issue.call_args_list[0][0][0]
        self.assertNotIn("parentIssueId", parent_params)
        self.assertEqual(parent_params["summary"], "【定期】20260828 タスク")

        child_params = [c[0][0] for c in mc.create_issue.call_args_list[1:]]
        self.assertEqual([p["summary"] for p in child_params],
                         ["手順1 バックアップ", "手順2 検証"])
        # 親の作成レスポンスの id が子の parentIssueId になる
        self.assertTrue(all(p["parentIssueId"] == 2001 for p in child_params))
        self.assertEqual([p["description"] for p in child_params], ["本文1", "本文2"])

    def test_child_inherits_issue_type_and_priority(self):
        _, mc, _ = self._run(source_children=self.SOURCE_CHILDREN)
        child_params = [c[0][0] for c in mc.create_issue.call_args_list[1:]]
        # 「タスク」=1 /「バグ」=2 と、コピー元の子課題の種別に追随する
        self.assertEqual([p["issueTypeId"] for p in child_params], [1, 2])
        self.assertEqual([p["priorityId"] for p in child_params], [3, 3])

    def test_unknown_child_type_falls_back_to_parent_type(self):
        children = [_child(101, "PROJ-2", "子", type_name="複製先に無い種別")]
        _, mc, _ = self._run(source_children=children)
        child_params = mc.create_issue.call_args_list[1][0][0]
        self.assertEqual(child_params["issueTypeId"], 1)  # 親と同じ「タスク」

    def test_existing_parent_children_are_diffed(self):
        """既存の親がある場合、その子課題と突き合わせて差分だけ反映する。"""
        existing_children = [
            _child(901, "PROJ-90", "手順1 バックアップ", "本文1"),   # 同一 → 変更なし
            _child(902, "PROJ-91", "手順2 検証", "古い本文"),        # 差分 → 更新
        ]
        outcome, mc, _ = self._run(
            existing=EXISTING_SAME,
            source_children=self.SOURCE_CHILDREN,
            existing_children=existing_children,
        )
        self.assertEqual(outcome, sut.OUTCOME_UPDATED)
        mc.create_issue.assert_not_called()
        mc.update_issue.assert_called_once_with("PROJ-91", {"description": "本文2"})

    def test_missing_child_is_created_under_existing_parent(self):
        existing_children = [_child(901, "PROJ-90", "手順1 バックアップ", "本文1")]
        outcome, mc, _ = self._run(
            existing=EXISTING_SAME,
            source_children=self.SOURCE_CHILDREN,
            existing_children=existing_children,
        )
        self.assertEqual(outcome, sut.OUTCOME_CREATED)
        mc.create_issue.assert_called_once()
        params = mc.create_issue.call_args[0][0]
        self.assertEqual(params["summary"], "手順2 検証")
        self.assertEqual(params["parentIssueId"], EXISTING_SAME["id"])

    def test_all_identical_returns_no_change(self):
        existing_children = [
            _child(901, "PROJ-90", "手順1 バックアップ", "本文1"),
            _child(902, "PROJ-91", "手順2 検証", "本文2"),
        ]
        outcome, mc, _ = self._run(
            existing=EXISTING_SAME,
            source_children=self.SOURCE_CHILDREN,
            existing_children=existing_children,
        )
        self.assertEqual(outcome, sut.OUTCOME_NO_CHANGE)
        mc.create_issue.assert_not_called()
        mc.update_issue.assert_not_called()
        mc.get_issue_types.assert_not_called()

    def test_child_no_change_error_is_treated_as_no_change(self):
        """子課題の更新で BacklogNoChangeError が出たら変更なし扱い。"""
        existing_children = [_child(901, "PROJ-90", "手順1 バックアップ", "古い本文")]
        patcher, mc = _mock_client(
            existing_issue=EXISTING_SAME,
            source_children=[self.SOURCE_CHILDREN[0]],
            existing_children=existing_children,
        )
        mc.update_issue.side_effect = sut.BacklogNoChangeError("変更なし")
        with patcher, patch("sys.stdout", new_callable=StringIO), tty(), \
             patch("builtins.input", return_value="y"):
            outcome = sut.run(_make_args(execute=True, date="20260828"), _make_config())
        mc.update_issue.assert_called_once()
        self.assertEqual(outcome, sut.OUTCOME_NO_CHANGE)

    def test_renamed_existing_child_is_warned_before_confirmation(self):
        """複製先で件名が変わった子課題は照合できず、重複作成の警告が出る。"""
        existing_children = [
            _child(901, "PROJ-90", "手順1 バックアップ", "本文1"),
            _child(902, "PROJ-91", "検証手順", "本文2"),  # 改名されている
        ]
        outcome, mc, out = self._run(
            existing=EXISTING_SAME,
            source_children=self.SOURCE_CHILDREN,
            existing_children=existing_children,
        )
        self.assertEqual(outcome, sut.OUTCOME_CREATED)
        self.assertIn("警告", out)
        self.assertIn("PROJ-91: 検証手順", out)
        # 動作自体は変わらず、重複した子課題が作られる
        self.assertEqual(mc.create_issue.call_args[0][0]["summary"], "手順2 検証")

    def test_cancel_after_warning_creates_nothing(self):
        existing_children = [_child(902, "PROJ-91", "検証手順", "本文2")]
        outcome, mc, out = self._run(
            existing=EXISTING_SAME,
            source_children=self.SOURCE_CHILDREN,
            existing_children=existing_children,
            answer="n",
        )
        self.assertIn("警告", out)
        self.assertEqual(outcome, sut.OUTCOME_SKIPPED)
        mc.create_issue.assert_not_called()

    def test_extra_child_in_target_does_not_warn_without_creation(self):
        """複製先に独自の子課題があっても、作成が無ければ警告しない。"""
        existing_children = [
            _child(901, "PROJ-90", "手順1 バックアップ", "本文1"),
            _child(902, "PROJ-91", "手順2 検証", "本文2"),
            _child(903, "PROJ-92", "現地対応メモ", "独自"),
        ]
        outcome, _, out = self._run(
            existing=EXISTING_SAME,
            source_children=self.SOURCE_CHILDREN,
            existing_children=existing_children,
        )
        self.assertEqual(outcome, sut.OUTCOME_NO_CHANGE)
        self.assertNotIn("警告", out)

    def test_cancel_skips_everything(self):
        outcome, mc, _ = self._run(source_children=self.SOURCE_CHILDREN, answer="n")
        self.assertEqual(outcome, sut.OUTCOME_SKIPPED)
        mc.create_issue.assert_not_called()
        mc.update_issue.assert_not_called()

    def test_single_confirmation_for_all_issues(self):
        """子課題が何件あっても確認は 1 回だけ。"""
        patcher, mc = _mock_client(source_children=self.SOURCE_CHILDREN)
        calls = []
        with patcher, patch("sys.stdout", new_callable=StringIO), tty(), \
             patch("builtins.input", side_effect=lambda p: calls.append(p) or "y"):
            sut.run(_make_args(execute=True, date="20260828"), _make_config())
        self.assertEqual(len(calls), 1)

    def test_dry_run_creates_nothing(self):
        outcome, mc, out = self._run(source_children=self.SOURCE_CHILDREN, execute=False)
        self.assertEqual(outcome, sut.OUTCOME_CREATED)
        mc.create_issue.assert_not_called()
        self.assertIn("手順1 バックアップ", out)
        self.assertIn("手順2 検証", out)

    def test_child_summary_template(self):
        cfg = _make_config()
        cfg["clone"]["child_summary_template"] = "{YYYYMMDD} {SOURCE_SUMMARY}"
        _, mc, _ = self._run(source_children=self.SOURCE_CHILDREN, config=cfg)
        child_params = [c[0][0] for c in mc.create_issue.call_args_list[1:]]
        self.assertEqual(
            [p["summary"] for p in child_params],
            ["20260828 手順1 バックアップ", "20260828 手順2 検証"],
        )

    # --- include_children ---

    def test_include_children_false_skips_children(self):
        cfg = _make_config()
        cfg["clone"]["include_children"] = False
        _, mc, _ = self._run(source_children=self.SOURCE_CHILDREN, config=cfg)
        mc.get_child_issues.assert_not_called()
        self.assertEqual(mc.create_issue.call_count, 1)  # 親のみ

    def test_source_without_children_behaves_as_before(self):
        outcome, mc, _ = self._run(source_children=[])
        self.assertEqual(outcome, sut.OUTCOME_CREATED)
        self.assertEqual(mc.create_issue.call_count, 1)

    def test_child_source_issue_warns_and_skips_children(self):
        """コピー元自身が子課題の場合は子を探さない（Backlog は 2 階層まで）。"""
        patcher, mc = _mock_client()
        mc.get_issue.return_value = {**SOURCE_ISSUE, "parentIssueId": 500}
        err = StringIO()
        with patcher, patch("sys.stdout", new_callable=StringIO), patch("sys.stderr", err), \
             tty(), patch("builtins.input", return_value="y"):
            sut.run(_make_args(execute=True, date="20260828"), _make_config())
        mc.get_child_issues.assert_not_called()
        self.assertIn("子課題のため", err.getvalue())

    def test_no_child_lookup_on_existing_parent_when_source_has_no_children(self):
        """コピー元に子が無ければ既存の親の子課題も引きに行かない。"""
        _, mc, _ = self._run(existing=EXISTING_DIFF, source_children=[])
        mc.get_child_issues.assert_called_once_with(SOURCE_ISSUE["id"])


class TestRunWithoutSummaryTemplate(unittest.TestCase):
    """summary_template 省略時はコピー元の件名をそのまま使う。"""

    def _run(self, *, search_results=None, config=None, answer="y"):
        patcher, mc = _mock_client()
        if search_results is not None:
            mc.search_issues_by_keyword.return_value = search_results
        out = StringIO()
        with patcher, patch("sys.stdout", out), patch("sys.stderr", new_callable=StringIO), \
             tty(), patch("builtins.input", return_value=answer):
            outcome = sut.run(
                _make_args(execute=True, date="20260828"),
                config or _make_config(summary_template=None),
            )
        return outcome, mc, out.getvalue()

    SAME_SUMMARY_ISSUE = {
        "id": 2002,
        "issueKey": "PROJ-50",
        "summary": SOURCE_ISSUE["summary"],
        "description": "古い本文",
    }

    def test_uses_source_summary_as_is(self):
        outcome, mc, _ = self._run(search_results=[])
        self.assertEqual(outcome, sut.OUTCOME_CREATED)
        params = mc.create_issue.call_args[0][0]
        self.assertEqual(params["summary"], SOURCE_ISSUE["summary"])

    def test_skips_duplicate_check_entirely(self):
        """複製先を特定する手掛かりが無いため重複チェックを行わない。"""
        outcome, mc, out = self._run(search_results=[self.SAME_SUMMARY_ISSUE])
        self.assertEqual(outcome, sut.OUTCOME_CREATED)
        mc.search_issues_by_keyword.assert_not_called()
        mc.update_issue.assert_not_called()
        mc.create_issue.assert_called_once()
        self.assertIn("重複判定", out)
        self.assertIn("行わない", out)

    def test_creates_again_on_second_run(self):
        """同じ設定で繰り返すたびに新しい課題が作られる。"""
        for _ in range(2):
            outcome, mc, _ = self._run(search_results=[self.SAME_SUMMARY_ISSUE])
            self.assertEqual(outcome, sut.OUTCOME_CREATED)
            mc.create_issue.assert_called_once()

    def test_children_are_all_created(self):
        """毎回新規作成なので既存の子課題を引きに行かない。"""
        patcher, mc = _mock_client(
            source_children=[_child(101, "PROJ-2", "手順1", "本文1")]
        )
        with patcher, patch("sys.stdout", new_callable=StringIO), \
             patch("sys.stderr", new_callable=StringIO), tty(), \
             patch("builtins.input", return_value="y"):
            sut.run(
                _make_args(execute=True, date="20260828"),
                _make_config(summary_template=None),
            )
        # コピー元の子課題のみ照会し、複製先の子課題は引かない
        self.assertEqual(
            [c[0][0] for c in mc.get_child_issues.call_args_list], [SOURCE_ISSUE["id"]]
        )
        self.assertEqual(mc.create_issue.call_count, 2)  # 親 1 + 子 1

    def test_explicit_source_summary_template_enables_duplicate_check(self):
        """明示的に {SOURCE_SUMMARY} と書いた場合は重複チェックする。"""
        cfg = _make_config(summary_template="{SOURCE_SUMMARY}")
        outcome, mc, _ = self._run(search_results=[self.SAME_SUMMARY_ISSUE], config=cfg)
        mc.search_issues_by_keyword.assert_called_once()
        self.assertEqual(outcome, sut.OUTCOME_UPDATED)
        mc.update_issue.assert_called_once_with("PROJ-50", {"description": "本文テキスト"})

    def test_explicit_source_summary_template_excludes_source_itself(self):
        """明示指定時はコピー元と同じ件名になるため、コピー元自身は除外する。"""
        cfg = _make_config(summary_template="{SOURCE_SUMMARY}")
        outcome, mc, _ = self._run(search_results=[SOURCE_ISSUE], config=cfg)
        self.assertEqual(outcome, sut.OUTCOME_CREATED)
        mc.update_issue.assert_not_called()

    def test_explicit_template_still_works(self):
        outcome, mc, _ = self._run(search_results=[], config=_make_config())
        params = mc.create_issue.call_args[0][0]
        self.assertEqual(params["summary"], "【定期】20260828 タスク")

    def test_template_can_combine_source_summary_and_date(self):
        cfg = _make_config(summary_template="{YYYYMMDD} {SOURCE_SUMMARY}")
        _, mc, _ = self._run(search_results=[], config=cfg)
        params = mc.create_issue.call_args[0][0]
        self.assertEqual(params["summary"], "20260828 テンプレート課題")


class TestRunWithTargetIssueKey(unittest.TestCase):
    """target_issue_key で複製先を明示する経路。"""

    TARGET = {
        "id": 3001,
        "issueKey": "PROJ-5",
        "summary": "複製先の件名",
        "description": "古い本文",
        "projectId": 10,
    }
    SOURCE_CHILDREN = [
        _child(101, "PROJ-2", "手順1 バックアップ", "本文1"),
        _child(102, "PROJ-3", "手順2 検証", "本文2"),
    ]

    def _run(self, *, target=None, source_children=None, existing_children=None,
             config=None, execute=True, answer="y"):
        patcher, mc = _mock_client(
            source_children=source_children, existing_children=existing_children
        )
        target = self.TARGET if target is None else target
        mc.get_issue.side_effect = lambda key: (
            SOURCE_ISSUE if key == "PROJ-1" else target
        )
        out = StringIO()
        with patcher, patch("sys.stdout", out), patch("sys.stderr", new_callable=StringIO), \
             tty(), patch("builtins.input", return_value=answer):
            outcome = sut.run(
                _make_args(execute=execute, date="20260828"),
                config or _make_config(target_issue_key="PROJ-5"),
            )
        return outcome, mc, out.getvalue()

    def test_updates_target_without_searching(self):
        outcome, mc, _ = self._run()
        self.assertEqual(outcome, sut.OUTCOME_UPDATED)
        mc.search_issues_by_keyword.assert_not_called()
        mc.update_issue.assert_called_once_with("PROJ-5", {"description": "本文テキスト"})

    def test_summary_is_not_changed(self):
        _, mc, out = self._run()
        params = mc.update_issue.call_args[0][1]
        self.assertNotIn("summary", params)
        self.assertIn("件名は変更しません", out)

    def test_no_change_when_description_matches(self):
        target = {**self.TARGET, "description": "本文テキスト"}
        outcome, mc, _ = self._run(target=target)
        self.assertEqual(outcome, sut.OUTCOME_NO_CHANGE)
        mc.update_issue.assert_not_called()

    def test_children_are_updated_one_by_one(self):
        existing_children = [
            _child(901, "PROJ-6", "手順1 バックアップ", "本文1"),   # 同一
            _child(902, "PROJ-7", "手順2 検証", "古い本文"),        # 差分
        ]
        outcome, mc, _ = self._run(
            source_children=self.SOURCE_CHILDREN, existing_children=existing_children
        )
        self.assertEqual(outcome, sut.OUTCOME_UPDATED)
        self.assertEqual(
            [c[0][0] for c in mc.update_issue.call_args_list], ["PROJ-5", "PROJ-7"]
        )
        mc.create_issue.assert_not_called()

    def test_missing_child_is_created_under_target(self):
        existing_children = [_child(901, "PROJ-6", "手順1 バックアップ", "本文1")]
        outcome, mc, _ = self._run(
            source_children=self.SOURCE_CHILDREN, existing_children=existing_children
        )
        self.assertEqual(outcome, sut.OUTCOME_CREATED)
        params = mc.create_issue.call_args[0][0]
        self.assertEqual(params["summary"], "手順2 検証")
        self.assertEqual(params["parentIssueId"], self.TARGET["id"])
        self.assertEqual(params["projectId"], self.TARGET["projectId"])

    def test_children_looked_up_on_source_then_target(self):
        _, mc, _ = self._run(source_children=self.SOURCE_CHILDREN, existing_children=[])
        called_ids = [c[0][0] for c in mc.get_child_issues.call_args_list]
        self.assertEqual(called_ids, [SOURCE_ISSUE["id"], self.TARGET["id"]])

    def test_missing_target_raises_config_error(self):
        patcher, mc = _mock_client()
        mc.get_issue.side_effect = lambda key: SOURCE_ISSUE if key == "PROJ-1" else None
        with patcher, patch("sys.stdout", new_callable=StringIO):
            with self.assertRaises(sut.ConfigError) as ctx:
                sut.run(
                    _make_args(execute=True, date="20260828"),
                    _make_config(target_issue_key="PROJ-999"),
                )
        self.assertIn("コピー先課題", str(ctx.exception))

    def test_target_in_another_project_raises_config_error(self):
        """プロジェクトを跨いだ複製は事故防止のため拒否する。"""
        other_project = {
            "id": 3001, "issueKey": "DEST-5", "summary": "別プロジェクトの課題",
            "description": "本文", "projectId": 99,
        }
        patcher, mc = _mock_client()
        mc.get_issue.side_effect = lambda key: (
            SOURCE_ISSUE if key == "PROJ-1" else other_project
        )
        with patcher, patch("sys.stdout", new_callable=StringIO):
            with self.assertRaises(sut.ConfigError) as ctx:
                sut.run(
                    _make_args(execute=True, date="20260828"),
                    _make_config(target_issue_key="DEST-5"),
                )
        message = str(ctx.exception)
        self.assertIn("プロジェクトが異なります", message)
        self.assertIn("PROJ", message)
        self.assertIn("DEST", message)
        mc.update_issue.assert_not_called()
        mc.create_issue.assert_not_called()

    def test_same_source_and_target_raises_config_error(self):
        patcher, mc = _mock_client()
        mc.get_issue.side_effect = lambda key: SOURCE_ISSUE
        with patcher, patch("sys.stdout", new_callable=StringIO):
            with self.assertRaises(sut.ConfigError) as ctx:
                sut.run(
                    _make_args(execute=True, date="20260828"),
                    _make_config(target_issue_key="PROJ-1"),
                )
        self.assertIn("同じ課題", str(ctx.exception))

    def test_dry_run_changes_nothing(self):
        outcome, mc, _ = self._run(execute=False)
        self.assertEqual(outcome, sut.OUTCOME_UPDATED)
        mc.update_issue.assert_not_called()

    def test_project_id_fetched_when_absent_on_target(self):
        target = {k: v for k, v in self.TARGET.items() if k != "projectId"}
        _, mc, _ = self._run(target=target)
        mc.get_project.assert_called_once_with("PROJ")

    def test_cancel_skips(self):
        outcome, mc, _ = self._run(answer="n")
        self.assertEqual(outcome, sut.OUTCOME_SKIPPED)
        mc.update_issue.assert_not_called()


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
# apply_cli_overrides テスト
# ===========================================================================


class TestApplyCliOverrides(unittest.TestCase):
    """設定ファイルの clone セクションをコマンドライン引数で上書きする。"""

    def _args(self, **kw):
        args = MagicMock()
        args.source_issue_key = kw.get("source_issue_key")
        args.target_issue_key = kw.get("target_issue_key")
        args.summary_template = kw.get("summary_template")
        args.no_summary_template = kw.get("no_summary_template", False)
        return args

    def _config(self, **clone):
        return {"backlog": {"space_host": "t.backlog.com", "api_key": "K"},
                "clone": dict(clone)}

    def test_no_overrides_returns_config_unchanged(self):
        cfg = self._config(source_issue_key="PROJ-1")
        self.assertIs(sut.apply_cli_overrides(cfg, self._args()), cfg)

    def test_source_issue_key_override(self):
        cfg = self._config(source_issue_key="PROJ-1")
        out = sut.apply_cli_overrides(cfg, self._args(source_issue_key="PROJ-9"))
        self.assertEqual(out["clone"]["source_issue_key"], "PROJ-9")
        # 元の dict は変更しない
        self.assertEqual(cfg["clone"]["source_issue_key"], "PROJ-1")

    def test_creates_clone_section_when_absent(self):
        """接続情報だけの設定ファイルでも指定できる。"""
        cfg = {"backlog": {"space_host": "t.backlog.com", "api_key": "K"}}
        out = sut.apply_cli_overrides(cfg, self._args(source_issue_key="PROJ-9"))
        self.assertEqual(out["clone"], {"source_issue_key": "PROJ-9"})
        sut.validate_config(out)  # 例外が出なければ OK

    def test_null_clone_section_is_replaced(self):
        cfg = {"backlog": {"space_host": "t.backlog.com", "api_key": "K"}, "clone": None}
        out = sut.apply_cli_overrides(cfg, self._args(source_issue_key="PROJ-9"))
        self.assertEqual(out["clone"], {"source_issue_key": "PROJ-9"})

    def test_summary_template_override(self):
        cfg = self._config(source_issue_key="PROJ-1", summary_template="旧")
        out = sut.apply_cli_overrides(cfg, self._args(summary_template="新{YYYYMMDD}"))
        self.assertEqual(out["clone"]["summary_template"], "新{YYYYMMDD}")

    def test_no_summary_template_drops_it(self):
        """設定ファイルの件名テンプレートを無視して単純複製にする。"""
        cfg = self._config(source_issue_key="PROJ-1", summary_template="【定期】{YYYYMMDD}")
        out = sut.apply_cli_overrides(cfg, self._args(no_summary_template=True))
        self.assertNotIn("summary_template", out["clone"])

    def test_no_summary_template_without_config_value(self):
        cfg = self._config(source_issue_key="PROJ-1")
        out = sut.apply_cli_overrides(cfg, self._args(no_summary_template=True))
        self.assertNotIn("summary_template", out["clone"])

    def test_target_issue_key_override(self):
        cfg = self._config(source_issue_key="PROJ-1")
        out = sut.apply_cli_overrides(cfg, self._args(target_issue_key="DEST-5"))
        self.assertEqual(out["clone"]["target_issue_key"], "DEST-5")

    def test_none_config_passes_through(self):
        """空ファイルは validate_config 側で弾く。"""
        self.assertIsNone(
            sut.apply_cli_overrides(None, self._args(source_issue_key="PROJ-9"))
        )

    def test_missing_source_key_message_mentions_cli_option(self):
        cfg = {"backlog": {"space_host": "t.backlog.com", "api_key": "K"}}
        with self.assertRaises(sut.ConfigError) as ctx:
            sut.validate_config(cfg)
        self.assertIn("--source-issue-key", str(ctx.exception))


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

    def test_override_options(self):
        args = self.parse([
            "--source-issue-key", "PROJ-1",
            "--target-issue-key", "DEST-5",
            "--summary-template", "【定期】{YYYYMMDD}",
        ])
        self.assertEqual(args.source_issue_key, "PROJ-1")
        self.assertEqual(args.target_issue_key, "DEST-5")
        self.assertEqual(args.summary_template, "【定期】{YYYYMMDD}")
        self.assertFalse(args.no_summary_template)

    def test_override_defaults_are_none(self):
        args = self.parse([])
        self.assertIsNone(args.source_issue_key)
        self.assertIsNone(args.target_issue_key)
        self.assertIsNone(args.summary_template)
        self.assertFalse(args.no_summary_template)

    def test_summary_template_options_are_mutually_exclusive(self):
        with patch("sys.stderr", new_callable=StringIO):
            with self.assertRaises(SystemExit):
                self.parse(["--summary-template", "x", "--no-summary-template"])

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
