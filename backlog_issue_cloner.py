"""
Backlog 課題クローンツール
==========================
指定した課題の description をコピーして新規課題を作成する CLI ツール。
親課題を指定した場合は、その子課題もまとめて複製する。

複製先の決め方は設定で 3 通りに変わる:
  どちらも省略             : 重複チェックせず毎回新規作成（単純複製）
  clone.summary_template   : その件名で探し、無ければ作成・あれば更新（定期作成）
  clone.target_issue_key   : 検索せずその課題（と子課題）を直接更新（直接更新）

使い方:
  python3 backlog_issue_cloner.py                    # ドライラン（デフォルト）
  python3 backlog_issue_cloner.py --execute          # 実際に作成/更新（対話確認あり）
  python3 backlog_issue_cloner.py --execute --yes    # 確認なしで実行（cron 向け）
  python3 backlog_issue_cloner.py --date 20260401    # 日付を指定
  python3 backlog_issue_cloner.py --execute --debug  # デバッグ出力付き
  python3 backlog_issue_cloner.py --config my.yaml   # 設定ファイルを指定
  python3 backlog_issue_cloner.py --detailed-exit-code
                                                     # 結果を終了コードで区別する

終了コード:
  0  正常終了
  2  設定エラー
  3  API / ネットワークエラー
  20 確認が得られずスキップした（--yes なしの非対話実行、またはユーザーが拒否）

  --detailed-exit-code を付けた場合、正常終了時は結果を区別して返す:
  0  変更なし / 10 新規作成した / 11 本文を更新した

依存:
  pip3 install pyyaml

詳細は README.md を参照。
"""

import argparse
import json
import ssl
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import yaml


# ===========================================================================
# 例外・定数
# ===========================================================================


class ConfigError(Exception):
    """設定ファイルの内容が不正、または前提条件を満たさないエラー。"""


class BacklogError(Exception):
    """Backlog API の呼び出しに失敗したエラー。"""

    def __init__(self, message: str, *, status: int | None = None, hint: str | None = None):
        super().__init__(message)
        self.status = status
        self.hint = hint


class BacklogNoChangeError(BacklogError):
    """更新内容が現在の課題と同一のため変更なしと判断されたエラー。"""


# 実行結果
OUTCOME_NO_CHANGE = "no_change"
OUTCOME_CREATED = "created"
OUTCOME_UPDATED = "updated"
OUTCOME_SKIPPED = "skipped"

# 終了コード
EXIT_OK = 0
EXIT_CONFIG_ERROR = 2
EXIT_API_ERROR = 3
EXIT_CREATED = 10
EXIT_UPDATED = 11
EXIT_SKIPPED = 20

# 課題の状態 ID（Backlog 共通）: 1=未対応 2=処理中 3=処理済み 4=完了
STATUS_IDS_OPEN = [1, 2, 3]

# リトライ対象の HTTP ステータス
RETRYABLE_STATUS = frozenset({429, 500, 502, 503, 504})

MATCH_MODES = ("substring", "exact")

# 件名テンプレートの既定値。コピー元の件名をそのまま使う。
DEFAULT_SUMMARY_TEMPLATE = "{SOURCE_SUMMARY}"

# backlog セクションで受け付ける数値設定と、その最小値
NUMERIC_SETTINGS = (
    ("timeout", 1),
    ("max_retries", 0),
    ("retry_backoff", 0),
    ("retry_max_delay", 0),
)


# HTTP ステータスごとの対処のヒント
HTTP_ERROR_HINTS = {
    400: "リクエストパラメータを確認してください。",
    401: "api_key を確認してください。",
    403: "api_key の権限を確認してください。",
    404: "space_host または project_key を確認してください。",
    429: "レート制限に達しました。しばらく待って再実行してください。",
}


def _flatten_params(params: dict) -> list[tuple[str, str]]:
    """
    パラメータ dict を (キー, 値) のペア列に展開する。
    リスト値は Backlog API が要求する key[]=v1&key[]=v2 形式に展開する。
    GET のクエリ文字列と POST/PATCH のボディの双方で使う。
    """
    pairs = []
    for key, value in params.items():
        if isinstance(value, list):
            pairs.extend((f"{key}[]", str(v)) for v in value)
        else:
            pairs.append((str(key), str(value)))
    return pairs


def _close_quietly(resource) -> None:
    """HTTPError などのレスポンスを例外を出さずに解放する。

    HTTPError は本文を読むために内部で一時ファイルを掴むため、
    破棄する前に明示的に閉じないと ResourceWarning が出る。
    """
    try:
        resource.close()
    except Exception:
        pass


# ===========================================================================
# Backlog API クライアント
# ===========================================================================


class BacklogClient:
    def __init__(
        self,
        space_host: str,
        api_key: str,
        ssl_verify: bool = True,
        base_path: str = "",
        debug: bool = False,
        timeout: int = 30,
        max_retries: int = 3,
        retry_backoff: float = 1.0,
        retry_max_delay: float = 60.0,
    ):
        base_path = "/" + base_path.strip("/") if base_path.strip("/") else ""
        self.base_url = f"https://{space_host}{base_path}/api/v2"
        self.api_key = api_key
        self.debug = debug
        self.timeout = timeout
        self.max_retries = max_retries
        self.retry_backoff = retry_backoff
        self.retry_max_delay = retry_max_delay

        if ssl_verify:
            self.ssl_context = None
        else:
            self.ssl_context = ssl.create_default_context()
            self.ssl_context.check_hostname = False
            self.ssl_context.verify_mode = ssl.CERT_NONE

    # ------------------------------------------------------------------
    # 内部ユーティリティ
    # ------------------------------------------------------------------

    def _handle_http_error(
        self,
        e: urllib.error.HTTPError,
        endpoint: str,
        *,
        raise_no_change: bool = False,
    ) -> None:
        """HTTPError を BacklogError に変換して送出する（常に例外を投げる）。"""
        detail = ""
        raw_body = ""
        errors: list = []
        try:
            raw_body = e.read().decode("utf-8")
            body = json.loads(raw_body)
            errors = body.get("errors", [])
            if errors:
                detail = " / ".join(
                    f"{err.get('message', '')}（code={err.get('code')}）"
                    for err in errors
                )
        except Exception:
            pass
        finally:
            _close_quietly(e)

        if raise_no_change and e.code == 400 and any(
            err.get("code") == 7 for err in errors
        ):
            raise BacklogNoChangeError(
                detail or "HTTP 400 / code 7（変更なしと判断）", status=e.code
            )

        message = f"API呼び出しに失敗しました（HTTP {e.code}）: {endpoint}"
        if detail:
            message += f"\n  詳細: {detail}"
        elif raw_body:
            message += f"\n  レスポンス: {raw_body[:500]}"

        raise BacklogError(message, status=e.code, hint=HTTP_ERROR_HINTS.get(e.code))

    def _retry_delay(self, error: urllib.error.HTTPError | None, attempt: int) -> float:
        """リトライまでの待機秒数。Retry-After ヘッダがあれば優先する。"""
        if error is not None and getattr(error, "headers", None):
            raw = error.headers.get("Retry-After")
            if raw:
                try:
                    return min(float(raw), self.retry_max_delay)
                except ValueError:
                    pass  # HTTP-date 形式は非対応。指数バックオフにフォールバック
        return min(self.retry_backoff * (2 ** attempt), self.retry_max_delay)

    def _sleep_before_retry(self, message: str, delay: float, attempt: int) -> None:
        print(
            f"  警告: {message}。{delay:.1f} 秒後に再試行します"
            f"（{attempt + 1}/{self.max_retries}）",
            file=sys.stderr,
        )
        time.sleep(delay)

    def _request(
        self,
        req: urllib.request.Request,
        endpoint: str,
        *,
        allow_404: bool = False,
        raise_no_change: bool = False,
        idempotent: bool = True,
    ):
        """
        リクエストを送信し、JSON をデコードして返す。
        一時的な失敗は max_retries 回まで指数バックオフで再試行する。
        allow_404 が True なら 404 時に None を返す。

        idempotent=False（課題の作成・更新）の場合、リクエストがサーバに届いて
        いないことが確実な失敗のみ再試行する。届いた後に失敗した可能性がある
        ケース（5xx・タイムアウト・接続断）で再送すると、サーバ側では成功して
        いたときに課題が二重に作られるため。
        """
        for attempt in range(self.max_retries + 1):
            try:
                with urllib.request.urlopen(
                    req, timeout=self.timeout, context=self.ssl_context
                ) as res:
                    return json.load(res)
            except urllib.error.HTTPError as e:
                # HTTPError は URLError のサブクラスなので必ず先に捕捉する
                if allow_404 and e.code == 404:
                    _close_quietly(e)
                    return None
                # 429 はリクエストが拒否された＝処理されていないことが確実なため、
                # 更新系でも安全に再試行できる。
                retryable = e.code in RETRYABLE_STATUS and (idempotent or e.code == 429)
                if retryable and attempt < self.max_retries:
                    delay = self._retry_delay(e, attempt)
                    _close_quietly(e)
                    self._sleep_before_retry(
                        f"HTTP {e.code}（{endpoint}）", delay, attempt
                    )
                    continue
                if e.code in RETRYABLE_STATUS and not idempotent:
                    print(
                        "  注意: 更新系リクエストのため再試行しません"
                        "（課題が二重に作られるのを避けるため）",
                        file=sys.stderr,
                    )
                self._handle_http_error(e, endpoint, raise_no_change=raise_no_change)
            except OSError as e:
                # URLError は接続・送信に失敗した場合で、リクエストはサーバに届いて
                # いないため更新系でも再送してよい。それ以外（TimeoutError や
                # ConnectionResetError など）は送信後に失敗した可能性があるため、
                # 冪等なリクエストに限って再試行する。
                unsent = isinstance(e, urllib.error.URLError)
                reason = getattr(e, "reason", None) or e
                if (idempotent or unsent) and attempt < self.max_retries:
                    delay = self._retry_delay(None, attempt)
                    self._sleep_before_retry(
                        f"接続エラー（{endpoint}）: {reason}", delay, attempt
                    )
                    continue
                hint = "space_host とネットワーク接続を確認してください。"
                if not idempotent and not unsent:
                    hint += (
                        "\n  更新系リクエストのため再試行していません。"
                        "Backlog 側で処理済みの可能性があるため、"
                        "課題の状態を確認してから再実行してください。"
                    )
                raise BacklogError(
                    f"ネットワークエラー（{endpoint}）: {reason}", hint=hint
                ) from e

    def _get(
        self, endpoint: str, params: dict = None, *, allow_404: bool = False
    ) -> dict | list | None:
        """GET リクエストを送信する。allow_404 が True なら 404 時に None を返す。"""
        params = dict(params or {})
        params["apiKey"] = self.api_key
        # クエリ文字列では空白を + ではなく %20 にするため quote を使う
        query = urllib.parse.urlencode(
            _flatten_params(params), quote_via=urllib.parse.quote
        )
        url = f"{self.base_url}{endpoint}?{query}"

        if self.debug:
            debug_parts = [p for p in query.split("&") if not p.startswith("apiKey=")]
            print(f"  [DEBUG GET] {endpoint} ?" + "&".join(debug_parts), file=sys.stderr)

        req = urllib.request.Request(url)
        return self._request(req, endpoint, allow_404=allow_404)

    def _send(
        self,
        method: str,
        endpoint: str,
        params: dict,
        *,
        raise_no_change: bool = False,
    ) -> dict:
        """フォームエンコードのボディを持つリクエスト（POST / PATCH）を送信する。"""
        url = f"{self.base_url}{endpoint}?apiKey={urllib.parse.quote(self.api_key)}"

        body_parts = _flatten_params(params)
        body = urllib.parse.urlencode(body_parts).encode("utf-8")

        if self.debug:
            print(f"  [DEBUG {method}] {endpoint}", file=sys.stderr)
            for k, v in body_parts:
                print(f"    {k}={v}", file=sys.stderr)

        req = urllib.request.Request(
            url,
            data=body,
            method=method,
            headers={"Content-Type": "application/x-www-form-urlencoded"},
        )
        return self._request(
            req, endpoint, raise_no_change=raise_no_change, idempotent=False
        )

    def _post(self, endpoint: str, params: dict) -> dict:
        return self._send("POST", endpoint, params)

    def _patch(self, endpoint: str, params: dict, *, raise_no_change: bool = False) -> dict:
        return self._send("PATCH", endpoint, params, raise_no_change=raise_no_change)

    # ------------------------------------------------------------------
    # マスターデータ取得
    # ------------------------------------------------------------------

    def get_project(self, project_key: str) -> dict:
        return self._get(f"/projects/{urllib.parse.quote(project_key)}")

    def get_issue_types(self, project_id_or_key) -> list:
        return self._get(f"/projects/{urllib.parse.quote(str(project_id_or_key))}/issueTypes")

    def get_priorities(self) -> list:
        return self._get("/priorities")

    # ------------------------------------------------------------------
    # 課題の取得・検索
    # ------------------------------------------------------------------

    def get_issue(self, issue_id_or_key: str) -> dict | None:
        """課題を1件取得。存在しない場合（404）は None を返す。"""
        return self._get(
            f"/issues/{urllib.parse.quote(str(issue_id_or_key))}",
            allow_404=True,
        )

    def _iter_issues(self, params: dict):
        """
        /issues をページネーションしながら遅延列挙する。
        ジェネレータのため、呼び出し元が途中で打ち切れば以降のページは取得しない。
        """
        offset = 0
        count = 100
        while True:
            page = self._get("/issues", {**params, "count": count, "offset": offset})
            if not page:
                return
            yield from page
            if len(page) < count:
                return
            offset += count
            time.sleep(0.3)

    def search_issues_by_keyword(
        self, project_id: int, keyword: str, status_ids: list | None = None
    ):
        """
        keyword でプロジェクト内の課題を遅延列挙する。
        status_ids を渡すとその状態の課題のみに絞り込む。
        Backlog の keyword 検索は summary + description を対象とするため、
        呼び出し元で summary のフィルタを行うこと。
        """
        params = {"projectId": [project_id], "keyword": keyword}
        if status_ids:
            params["statusId"] = list(status_ids)
        return self._iter_issues(params)

    def get_child_issues(self, parent_issue_id: int):
        """
        親課題に紐づく子課題を遅延列挙する。
        親課題で絞り込むため状態によるフィルタは行わない
        （完了済みの子課題を再作成してしまうのを避けるため）。
        """
        return self._iter_issues({"parentIssueId": [parent_issue_id]})

    # ------------------------------------------------------------------
    # 課題の作成・更新
    # ------------------------------------------------------------------

    def create_issue(self, params: dict) -> dict:
        """課題を新規作成する。必須: projectId, summary, issueTypeId, priorityId"""
        return self._post("/issues", params)

    def update_issue(self, issue_id_or_key: str, params: dict) -> dict:
        """
        既存課題を更新する。変更内容が同一の場合は BacklogNoChangeError を raise する。
        """
        return self._patch(
            f"/issues/{urllib.parse.quote(str(issue_id_or_key))}",
            params,
            raise_no_change=True,
        )


# ===========================================================================
# 設定ファイル
# ===========================================================================


def load_config(config_path: str) -> dict:
    path = Path(config_path)
    if not path.exists():
        raise ConfigError(f"設定ファイルが見つかりません: {config_path}")
    with open(path, encoding="utf-8") as f:
        return yaml.safe_load(f)


def _require_section(config: dict, name: str) -> dict:
    """必須セクションを取り出す。空・型不正なら ConfigError。"""
    section = config.get(name)
    if section is None:
        raise ConfigError(
            f"config.yaml に {name}: セクションがありません。"
            "config.sample.yaml を参考に記述してください。"
        )
    if not isinstance(section, dict):
        raise ConfigError(
            f"config.yaml の {name}: セクションの形式が不正です（マッピングが必要です）。"
        )
    return section


def apply_cli_overrides(config: dict, args: argparse.Namespace) -> dict:
    """
    コマンドライン引数で設定ファイルの clone セクションを上書きする。
    設定ファイルには接続情報だけ置き、複製の対象は実行時に指定する使い方を可能にする。
    """
    if not isinstance(config, dict):
        return config  # 空ファイルなどは validate_config に任せる

    overrides = {
        "source_issue_key": getattr(args, "source_issue_key", None),
        "target_issue_key": getattr(args, "target_issue_key", None),
        "summary_template": getattr(args, "summary_template", None),
    }
    drop_template = bool(getattr(args, "no_summary_template", False))
    if not any(overrides.values()) and not drop_template:
        return config

    section = config.get("clone")
    clone = dict(section) if isinstance(section, dict) else {}
    for key, value in overrides.items():
        if value:
            clone[key] = value
    if drop_template:
        clone.pop("summary_template", None)

    # target_issue_key を指定した実行では複製先がその課題に決まるため、
    # 設定ファイル側の target_project_key は意味を持たない。
    # 残すと「同時に指定できません」で弾かれるので取り下げる。
    if overrides["target_issue_key"] and clone.pop("target_project_key", None):
        print(
            "警告: --target-issue-key を指定したため、"
            "設定ファイルの clone.target_project_key は無視します。",
            file=sys.stderr,
        )

    return {**config, "clone": clone}


def validate_config(config: dict) -> None:
    # yaml.safe_load は空ファイルに対して None を返すため最初に弾く
    if not isinstance(config, dict):
        raise ConfigError(
            "設定ファイルの内容が空か、形式が不正です。"
            "config.sample.yaml を参考に記述してください。"
        )

    b = _require_section(config, "backlog")
    for key, placeholder in [
        ("space_host", "yourcompany.backlog.com"),
        ("api_key", "YOUR_API_KEY_HERE"),
    ]:
        val = b.get(key, "")
        if not val or val == placeholder:
            raise ConfigError(f"config.yaml の backlog.{key} を設定してください。")

    c = config.get("clone")
    if c is None:
        raise ConfigError(
            "コピー元の課題が指定されていません。config.yaml の clone.source_issue_key に"
            "設定するか、--source-issue-key で指定してください"
            "（対話形式で選ぶ場合は menu.py を使ってください）。"
        )
    if not isinstance(c, dict):
        raise ConfigError("config.yaml の clone: セクションの形式が不正です（マッピングが必要です）。")

    # source_issue_key はサンプル値（PROJ-123）かどうかを見ない。
    # --source-issue-key や menu.py から実在のキーとして渡されることがあり、
    # 未編集のまま実行した場合も「コピー元課題が見つかりません」で十分に伝わるため。
    if not c.get("source_issue_key"):
        raise ConfigError(
            "コピー元の課題が指定されていません。config.yaml の clone.source_issue_key に"
            "設定するか、--source-issue-key で指定してください。"
        )
    # summary_template は省略可（省略時はコピー元の件名をそのまま使う）。
    # ただし空文字を指定した場合は件名が空になってしまうため弾く。
    if "summary_template" in c and not c["summary_template"]:
        raise ConfigError(
            "config.yaml の clone.summary_template を空にはできません。"
            "（省略すればコピー元の件名をそのまま使います）"
        )

    if "target_issue_key" in c:
        if not c["target_issue_key"]:
            raise ConfigError(
                "config.yaml の clone.target_issue_key を空にはできません。"
                "（件名で複製先を探す場合はこの項目ごと省略してください）"
            )
        if c.get("target_project_key"):
            raise ConfigError(
                "config.yaml の clone.target_issue_key と clone.target_project_key は"
                "同時に指定できません。"
                "（target_issue_key を指定した場合、複製先はその課題のプロジェクトになります）"
            )

    match_mode = c.get("match_mode", "substring")
    if match_mode not in MATCH_MODES:
        raise ConfigError(
            f"config.yaml の clone.match_mode が不正です: {match_mode!r}"
            f"（利用可能: {', '.join(MATCH_MODES)}）"
        )

    # 数値設定: bool は int のサブクラスなので明示的に除外する
    for key, minimum in NUMERIC_SETTINGS:
        if key not in b:
            continue
        val = b[key]
        if isinstance(val, bool) or not isinstance(val, (int, float)) or val < minimum:
            raise ConfigError(
                f"config.yaml の backlog.{key} は {minimum} 以上の数値で"
                f"指定してください: {val!r}"
            )

    if "child_summary_template" in c and not c["child_summary_template"]:
        raise ConfigError(
            "config.yaml の clone.child_summary_template を空にはできません。"
            "（省略時は {SOURCE_SUMMARY}、つまりコピー元の子課題の件名をそのまま使います）"
        )

    # 真偽値設定: "false" のような文字列を真と誤解しないよう型を確認する
    for section_name, section, key in (
        ("backlog", b, "ssl_verify"),
        ("clone", c, "include_closed"),
        ("clone", c, "include_children"),
    ):
        if key in section and not isinstance(section[key], bool):
            raise ConfigError(
                f"config.yaml の {section_name}.{key} は true / false で"
                f"指定してください: {section[key]!r}"
            )


# ===========================================================================
# ユーティリティ
# ===========================================================================


def resolve_date(date_arg: str | None) -> str:
    """--date 引数または今日の日付を YYYYMMDD 形式で返す。"""
    if date_arg:
        try:
            datetime.strptime(date_arg, "%Y%m%d")
            return date_arg
        except ValueError:
            raise ConfigError(
                f"--date の形式が不正です（YYYYMMDD 形式で指定してください）: {date_arg}"
            )
    return datetime.now().strftime("%Y%m%d")


def fetch_issue_types(client: BacklogClient, project_key: str) -> list:
    """プロジェクトの種別一覧を取得する。空なら ConfigError。"""
    types = client.get_issue_types(project_key)
    if not types:
        raise ConfigError(f"プロジェクト {project_key} の種別が取得できませんでした。")
    return types


def fetch_priorities(client: BacklogClient) -> list:
    """優先度一覧を取得する。空なら ConfigError。"""
    priorities = client.get_priorities()
    if not priorities:
        raise ConfigError("優先度一覧が取得できませんでした。")
    return priorities


def resolve_issue_type_id(types: list, name: str | None) -> tuple[int, str]:
    """種別IDと種別名を返す。見つからない場合は警告して最初の種別にフォールバック。"""
    if name:
        matched = [t for t in types if t["name"] == name]
        if matched:
            return matched[0]["id"], matched[0]["name"]
        available = [t["name"] for t in types]
        print(
            f"警告: 種別「{name}」が見つかりません。最初の種別「{types[0]['name']}」を使用します。"
            f"（利用可能: {available}）",
            file=sys.stderr,
        )
    return types[0]["id"], types[0]["name"]


def resolve_priority_id(priorities: list, name: str | None) -> tuple[int, str]:
    """優先度IDと優先度名を返す。見つからない場合は「中」→ 最初の優先度にフォールバック。"""
    if name:
        matched = [p for p in priorities if p["name"] == name]
        if matched:
            return matched[0]["id"], matched[0]["name"]
        available = [p["name"] for p in priorities]
        print(
            f"警告: 優先度「{name}」が見つかりません。（利用可能: {available}）",
            file=sys.stderr,
        )
    # フォールバック: "中" を探す
    chuu = [p for p in priorities if p["name"] == "中"]
    if chuu:
        return chuu[0]["id"], chuu[0]["name"]
    return priorities[0]["id"], priorities[0]["name"]


def find_existing_by_summary(
    client: BacklogClient,
    project_id: int,
    summary: str,
    *,
    match_mode: str = "substring",
    status_ids: list | None = None,
    exclude_id: int | None = None,
) -> dict | None:
    """
    件名が summary にマッチする課題を返す。なければ None。
    keyword 検索は summary + description を対象とするため、件名側でフィルタする。
    match_mode="exact" なら完全一致、"substring" なら部分一致。
    status_ids を渡すとその状態の課題のみを検索対象にする。
    exclude_id を渡すとその課題を検索結果から除外する。summary_template を
    省略するとコピー元と同じ件名になり、コピー元自身がヒットしてしまうため。
    検索結果は遅延列挙されるため、最初にマッチした時点で以降のページは取得しない。
    """
    for issue in client.search_issues_by_keyword(project_id, summary, status_ids):
        if exclude_id is not None and issue.get("id") == exclude_id:
            continue
        if summary_matches(summary, issue.get("summary", ""), match_mode):
            return issue
    return None


def summary_matches(wanted: str, candidate: str, match_mode: str) -> bool:
    """件名が一致するか。match_mode="exact" なら完全一致、"substring" なら部分一致。"""
    if match_mode == "exact":
        return candidate == wanted
    return wanted in candidate


# ===========================================================================
# 子課題の複製計画
# ===========================================================================


@dataclass
class ChildPlan:
    """1 件の子課題に対して行う操作。"""

    source: dict            # コピー元の子課題
    summary: str            # 作成・照合に使う件名
    action: str             # OUTCOME_CREATED / OUTCOME_UPDATED / OUTCOME_NO_CHANGE
    existing: dict | None = None  # 複製先に既にある子課題


def build_summary(template: str, source_summary: str, date_str: str) -> str:
    """
    件名テンプレートを展開する。親課題・子課題の双方で使う。
      {SOURCE_SUMMARY} : コピー元の件名
      {YYYYMMDD}       : --date または今日の日付
    """
    return template.replace("{SOURCE_SUMMARY}", source_summary).replace(
        "{YYYYMMDD}", date_str
    )


def build_child_plans(
    source_children: list,
    existing_children: list,
    *,
    template: str,
    date_str: str,
    match_mode: str,
) -> list[ChildPlan]:
    """
    コピー元の子課題と複製先の既存子課題を突き合わせ、各子課題の操作を決める。
    照合は複製先の親課題に紐づく子課題の中だけで行うため、
    プロジェクト全体のキーワード検索は不要。
    """
    plans = []
    used = set()
    for source in source_children:
        summary = build_summary(template, source.get("summary", ""), date_str)
        match = None
        for i, existing in enumerate(existing_children):
            if i in used:
                continue
            if summary_matches(summary, existing.get("summary", ""), match_mode):
                match = (i, existing)
                break
        if match is None:
            plans.append(ChildPlan(source=source, summary=summary, action=OUTCOME_CREATED))
            continue
        index, existing = match
        used.add(index)
        same = (existing.get("description") or "") == (source.get("description") or "")
        plans.append(
            ChildPlan(
                source=source,
                summary=summary,
                action=OUTCOME_NO_CHANGE if same else OUTCOME_UPDATED,
                existing=existing,
            )
        )
    return plans


def _issue_ref(issue: dict):
    """課題を識別するキー。id があれば id、無ければ issueKey。"""
    return issue.get("id", issue.get("issueKey"))


def find_unmatched_children(existing_children: list, plans: list[ChildPlan]) -> list:
    """
    複製先にあるが、コピー元のどの子課題とも照合できなかった子課題を返す。
    複製先で件名が変更されると照合に失敗し、同じ内容の子課題が
    重複して作られるため、実行前に気付けるようにする。
    """
    matched = {_issue_ref(p.existing) for p in plans if p.existing is not None}
    return [c for c in existing_children if _issue_ref(c) not in matched]


def resolve_child_issue_type(
    issue_types: list, source_child: dict, fallback: tuple[int, str]
) -> tuple[int, str]:
    """
    子課題の種別を複製先プロジェクトで解決する。
    コピー元と同名の種別があればそれを使い、無ければ親と同じ種別にフォールバックする。
    """
    name = (source_child.get("issueType") or {}).get("name")
    if name:
        for t in issue_types:
            if t["name"] == name:
                return t["id"], t["name"]
    return fallback


def aggregate_outcome(actions: list[str]) -> str:
    """親と子の操作をまとめて 1 つの実行結果にする。"""
    for outcome in (OUTCOME_SKIPPED, OUTCOME_CREATED, OUTCOME_UPDATED):
        if outcome in actions:
            return outcome
    return OUTCOME_NO_CHANGE


# ===========================================================================
# 確認プロンプト
# ===========================================================================


def _ask(prompt: str, assume_yes: bool) -> bool:
    """[y/N] の確認を取る。--yes 指定時は常に True、非対話時は False。"""
    if assume_yes:
        print(f"{prompt} y（--yes 指定）")
        return True
    if not sys.stdin.isatty():
        print(
            "  スキップ: 非対話環境のため確認できません。"
            "自動実行する場合は --yes を指定してください。",
            file=sys.stderr,
        )
        return False
    try:
        answer = input(prompt).strip().lower()
    except EOFError:
        answer = ""
    return answer in ("y", "yes")


ACTION_LABELS = {
    OUTCOME_CREATED: "新規作成",
    OUTCOME_UPDATED: "本文を更新",
    OUTCOME_NO_CHANGE: "変更なし",
}


def print_plan(
    parent_action: str,
    parent_summary: str,
    parent_existing: dict | None,
    child_plans: list[ChildPlan],
    unmatched_children: list | None = None,
) -> None:
    """これから行う操作の一覧を表示する。"""
    print("\n実行内容:")
    where = f"（{parent_existing['issueKey']}）" if parent_existing else ""
    print(f"  [親] {ACTION_LABELS[parent_action]:8} {parent_summary}{where}")
    for plan in child_plans:
        where = f"（{plan.existing['issueKey']}）" if plan.existing else ""
        print(f"  [子] {ACTION_LABELS[plan.action]:8} {plan.summary}{where}")

    # 新規作成が発生し、かつ複製先に照合できなかった子課題がある場合のみ警告する。
    # 複製先で件名が変更されていると照合に失敗し、同じ内容の子課題が
    # 重複して作られるため。作成が無ければ重複しないので黙っておく。
    if unmatched_children and any(p.action == OUTCOME_CREATED for p in child_plans):
        print("\n  警告: 複製先に照合できなかった子課題があります:")
        for issue in unmatched_children:
            print(f"      {issue.get('issueKey')}: {issue.get('summary', '')}")
        print(
            "    複製先で件名が変更されていると照合できず、"
            "同じ内容の子課題が重複して作られます。"
        )
        print("    重複させたくない場合はここで中断してください。")


def confirm_plan(
    parent_action: str, child_plans: list[ChildPlan], assume_yes: bool = False
) -> bool:
    """作成・更新が 1 件でもあれば確認を取る。何もしない場合は確認しない。"""
    actions = [parent_action] + [p.action for p in child_plans]
    creates = actions.count(OUTCOME_CREATED)
    updates = actions.count(OUTCOME_UPDATED)
    if not creates and not updates:
        return True
    parts = []
    if creates:
        parts.append(f"新規作成 {creates} 件")
    if updates:
        parts.append(f"本文更新 {updates} 件")
    return _ask(
        f"  Backlog に反映しますか？（{' / '.join(parts)}） [y/N]: ", assume_yes
    )


# ===========================================================================
# メインロジック
# ===========================================================================


def run(args: argparse.Namespace, config: dict) -> str:
    """クローン処理を実行し、OUTCOME_* のいずれかを返す。"""
    dry_run = not args.execute
    assume_yes = bool(getattr(args, "yes", False))
    backlog_cfg = config["backlog"]
    clone_cfg = config["clone"]

    # 1. 日付解決（件名テンプレートの展開はコピー元を取得した後）
    date_str = resolve_date(args.date)

    # 2. BacklogClient 初期化
    client = BacklogClient(
        space_host=backlog_cfg["space_host"],
        api_key=backlog_cfg["api_key"],
        ssl_verify=backlog_cfg.get("ssl_verify", True),
        base_path=backlog_cfg.get("base_path", ""),
        debug=args.debug,
        timeout=backlog_cfg.get("timeout", 30),
        max_retries=backlog_cfg.get("max_retries", 3),
        retry_backoff=backlog_cfg.get("retry_backoff", 1.0),
        retry_max_delay=backlog_cfg.get("retry_max_delay", 60.0),
    )

    # 3. コピー元課題を取得
    source_key = clone_cfg["source_issue_key"]
    print(f"コピー元課題を取得中: {source_key}")
    source_issue = client.get_issue(source_key)
    if source_issue is None:
        raise ConfigError(f"コピー元課題「{source_key}」が見つかりません。")
    source_desc = source_issue.get("description") or ""

    # 3b. コピー元の子課題を取得
    include_children = bool(clone_cfg.get("include_children", True))
    source_children = []
    if include_children:
        if source_issue.get("parentIssueId"):
            # Backlog の親子関係は 2 階層までで、子課題は子を持てない
            print(
                f"警告: {source_key} は子課題のため、子課題の複製は行いません。",
                file=sys.stderr,
            )
        else:
            print("コピー元の子課題を取得中...")
            source_children = list(client.get_child_issues(source_issue["id"]))
            print(f"  子課題: {len(source_children)} 件")

    # 4. 重複検出の条件を確定
    match_mode = clone_cfg.get("match_mode", "substring")
    include_closed = bool(clone_cfg.get("include_closed", False))
    status_ids = None if include_closed else STATUS_IDS_OPEN

    # 5. コピー先を確定する
    target_issue_key = clone_cfg.get("target_issue_key")
    summary_template = clone_cfg.get("summary_template")
    # summary_template も target_issue_key も無い場合、複製先を特定する手掛かりが
    # ないため重複チェックを行わず、実行のたびに新しい課題を作る。
    # コピー元と同じ件名で重複チェックしたい場合は summary_template に
    # "{SOURCE_SUMMARY}" を明示する。
    always_create = not target_issue_key and not summary_template

    if target_issue_key:
        # 5a. コピー先が明示されている場合は件名で探さず直接取得する
        print(f"コピー先課題を取得中: {target_issue_key}")
        existing = client.get_issue(target_issue_key)
        if existing is None:
            raise ConfigError(f"コピー先課題「{target_issue_key}」が見つかりません。")
        if existing.get("id") == source_issue.get("id"):
            raise ConfigError(
                f"コピー元とコピー先が同じ課題です: {source_key}"
            )
        summary = existing.get("summary", "")  # 件名は変更しない
        target_project_key = existing["issueKey"].rsplit("-", 1)[0]
        project_id = existing.get("projectId")
        if project_id is None:
            project_id = client.get_project(target_project_key)["id"]
    else:
        # 5b. 件名テンプレートを展開する
        summary = build_summary(
            summary_template or DEFAULT_SUMMARY_TEMPLATE,
            source_issue.get("summary", ""),
            date_str,
        )
        # Backlog API の単一課題レスポンスには projectId（数値）のみ含まれ
        # project オブジェクトはない。コピー元と同じプロジェクトなら issueKey
        #（例: PROJ-123）のプレフィックスをキーとし、ID は取得済みの
        # source_issue["projectId"] を流用して API 呼び出しを 1 回節約する。
        override_key = clone_cfg.get("target_project_key")
        if override_key:
            target_project_key = override_key
            print(f"対象プロジェクトを取得中: {target_project_key}")
            project_id = client.get_project(target_project_key)["id"]
        else:
            target_project_key = source_issue["issueKey"].rsplit("-", 1)[0]
            project_id = source_issue.get("projectId")
            if project_id is None:
                print(f"対象プロジェクトを取得中: {target_project_key}")
                project_id = client.get_project(target_project_key)["id"]

    # 6. 解決済み設定値を表示
    # 種別・優先度は新規作成でしか使わないため、ここでは解決しない。
    # 更新・変更なしの経路で無駄な API 呼び出しを 2 回省くため。
    prefix = "[DRY RUN] " if dry_run else ""
    print(f"\n{prefix}設定値:")
    print(f"  件名        : {summary}")
    print(f"  コピー元    : {source_key}")
    print(f"  対象PJ      : {target_project_key} (id={project_id})")
    print(f"  本文文字数  : {len(source_desc)} 文字")
    if target_issue_key:
        print(f"  コピー先    : {target_issue_key}（件名は変更しません）")
        print(f"  子課題照合  : {match_mode}")
    elif always_create:
        print("  重複判定    : 行わない（毎回新しい課題を作成）")
    else:
        print(f"  重複判定    : {match_mode}"
              f"（完了済み課題を{'含む' if include_closed else '除く'}）")

    # 7. 複製先を検索する（コピー先が明示されている場合は 5a で確定済み）
    if not target_issue_key:
        if always_create:
            existing = None
        else:
            print(f"\n既存課題を検索中（件名: {summary!r}）...")
            existing = find_existing_by_summary(
                client, project_id, summary,
                match_mode=match_mode,
                status_ids=status_ids,
                # "{SOURCE_SUMMARY}" 指定時はコピー元と同じ件名になるため自分自身を除く
                exclude_id=source_issue.get("id"),
            )

    # 8. 親課題の操作を決める
    if existing is None:
        parent_action = OUTCOME_CREATED
        existing_children = []
    else:
        print(f"既存課題あり: {existing['issueKey']}")
        same = (existing.get("description") or "") == source_desc
        parent_action = OUTCOME_NO_CHANGE if same else OUTCOME_UPDATED
        # 既存の親に紐づく子課題を照合対象にする
        existing_children = (
            list(client.get_child_issues(existing["id"])) if source_children else []
        )

    # 9. 子課題の操作を決める
    child_plans = build_child_plans(
        source_children,
        existing_children,
        template=clone_cfg.get("child_summary_template", "{SOURCE_SUMMARY}"),
        date_str=date_str,
        match_mode=match_mode,
    )

    print_plan(
        parent_action,
        summary,
        existing,
        child_plans,
        find_unmatched_children(existing_children, child_plans),
    )

    all_actions = [parent_action] + [p.action for p in child_plans]
    if OUTCOME_CREATED not in all_actions and OUTCOME_UPDATED not in all_actions:
        print("\n変更はありません。")
        return OUTCOME_NO_CHANGE

    # 10. 作成があるときだけ種別・優先度を解決する
    issue_types = priorities = None
    issue_type_id = priority_id = None
    if OUTCOME_CREATED in all_actions:
        issue_types = fetch_issue_types(client, target_project_key)
        priorities = fetch_priorities(client)
        issue_type_id, issue_type_name = resolve_issue_type_id(
            issue_types, clone_cfg.get("issue_type")
        )
        priority_id, priority_name = resolve_priority_id(
            priorities, clone_cfg.get("priority")
        )
        print(f"  種別（親）  : {issue_type_name} (id={issue_type_id})")
        print(f"  優先度（親）: {priority_name} (id={priority_id})")

    if dry_run:
        return aggregate_outcome(all_actions)

    # 11. 確認
    if not confirm_plan(parent_action, child_plans, assume_yes):
        print("スキップ（キャンセル）")
        return OUTCOME_SKIPPED

    # 12. 親課題を作成・更新して、子課題を紐づける親の ID を確定する
    if parent_action == OUTCOME_CREATED:
        created = client.create_issue({
            "projectId": project_id,
            "summary": summary,
            "issueTypeId": issue_type_id,
            "priorityId": priority_id,
            "description": source_desc,
        })
        parent_id = created["id"]
        print(f"作成完了: {created['issueKey']} — {created['summary']}")
    else:
        parent_id = existing["id"]
        if parent_action == OUTCOME_UPDATED:
            try:
                updated = client.update_issue(
                    existing["issueKey"], {"description": source_desc}
                )
                print(f"更新完了: {updated['issueKey']} — {updated['summary']}")
            except BacklogNoChangeError:
                print(f"スキップ（変更なし）: {existing['issueKey']}")
                parent_action = OUTCOME_NO_CHANGE

    # 13. 子課題を作成・更新する
    done_actions = [parent_action]
    for plan in child_plans:
        done_actions.append(
            _apply_child_plan(
                client, plan, parent_id, project_id, issue_types,
                (issue_type_id, priority_id),
            )
        )

    return aggregate_outcome(done_actions)


def _apply_child_plan(
    client: BacklogClient,
    plan: ChildPlan,
    parent_id: int,
    project_id: int,
    issue_types: list | None,
    parent_defaults: tuple,
) -> str:
    """1 件の子課題に対して計画した操作を実行し、実際の結果を返す。"""
    if plan.action == OUTCOME_NO_CHANGE:
        return OUTCOME_NO_CHANGE

    source_desc = plan.source.get("description") or ""

    if plan.action == OUTCOME_UPDATED:
        try:
            updated = client.update_issue(
                plan.existing["issueKey"], {"description": source_desc}
            )
        except BacklogNoChangeError:
            print(f"  [子] スキップ（変更なし）: {plan.existing['issueKey']}")
            return OUTCOME_NO_CHANGE
        print(f"  [子] 更新完了: {updated['issueKey']} — {updated['summary']}")
        return OUTCOME_UPDATED

    # 種別はコピー元の子課題に合わせ、優先度は Backlog 共通のため ID をそのまま使う
    fallback_type_id, fallback_priority_id = parent_defaults
    type_id, _ = resolve_child_issue_type(
        issue_types or [], plan.source, (fallback_type_id, "")
    )
    priority_id = (plan.source.get("priority") or {}).get("id") or fallback_priority_id
    created = client.create_issue({
        "projectId": project_id,
        "summary": plan.summary,
        "issueTypeId": type_id,
        "priorityId": priority_id,
        "description": source_desc,
        "parentIssueId": parent_id,
    })
    print(f"  [子] 作成完了: {created['issueKey']} — {created['summary']}")
    return OUTCOME_CREATED


def exit_code_for(outcome: str, detailed: bool) -> int:
    """実行結果を終了コードに変換する。"""
    if outcome == OUTCOME_SKIPPED:
        return EXIT_SKIPPED
    if not detailed:
        return EXIT_OK
    return {
        OUTCOME_NO_CHANGE: EXIT_OK,
        OUTCOME_CREATED: EXIT_CREATED,
        OUTCOME_UPDATED: EXIT_UPDATED,
    }[outcome]


# ===========================================================================
# エントリポイント
# ===========================================================================


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Backlog 課題クローンツール",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
例:
  python3 backlog_issue_cloner.py                          # ドライラン（デフォルト）
  python3 backlog_issue_cloner.py --execute                # 実際に作成/更新
  python3 backlog_issue_cloner.py --execute --yes          # 確認なしで実行（cron 向け）
  python3 backlog_issue_cloner.py --date 20260401          # 日付を指定
  python3 backlog_issue_cloner.py --execute --debug        # デバッグ出力付きで実行
  python3 backlog_issue_cloner.py --config my_config.yaml  # 設定ファイルを指定

  # 作成/更新が起きたかを終了コードで判定する
  python3 backlog_issue_cloner.py --execute --yes --detailed-exit-code

終了コード:
  0  正常終了 / 2  設定エラー / 3  API・ネットワークエラー
  20 確認が得られずスキップ（--yes なしの非対話実行、またはユーザーが拒否）
  --detailed-exit-code 指定時は正常終了を細分化: 0 変更なし / 10 作成 / 11 更新
""",
    )
    default_config = str(Path(__file__).parent / "config.yaml")
    parser.add_argument(
        "--config",
        default=default_config,
        help="設定ファイルのパス（デフォルト: config.yaml）",
    )
    parser.add_argument(
        "--date",
        default=None,
        metavar="YYYYMMDD",
        help="日付（YYYYMMDD 形式）。省略時は今日の日付",
    )
    parser.add_argument(
        "--execute",
        action="store_true",
        help="実際に API を呼び出す（省略時はドライラン）",
    )
    parser.add_argument(
        "-y", "--yes",
        action="store_true",
        help="確認プロンプトを出さずに実行する（cron などの自動実行向け）",
    )
    parser.add_argument(
        "--detailed-exit-code",
        action="store_true",
        help="正常終了時に結果を終了コードで区別する（0 変更なし / 10 作成 / 11 更新）",
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="API リクエストの詳細を表示する",
    )

    override = parser.add_argument_group(
        "設定の上書き",
        "設定ファイルの clone セクションを実行時に上書きする"
        "（設定ファイルには接続情報だけ置く運用向け）",
    )
    override.add_argument(
        "--source-issue-key",
        metavar="KEY",
        help="コピー元の課題キー（例: PROJ-123）",
    )
    override.add_argument(
        "--target-issue-key",
        metavar="KEY",
        help="複製先の課題キー。指定すると件名で検索せずこの課題を直接更新する",
    )
    template = override.add_mutually_exclusive_group()
    template.add_argument(
        "--summary-template",
        metavar="TEMPLATE",
        help="件名テンプレート（{SOURCE_SUMMARY} / {YYYYMMDD} が使える）",
    )
    template.add_argument(
        "--no-summary-template",
        action="store_true",
        help="設定ファイルの件名テンプレートを無視し、重複チェックせず毎回新規作成する",
    )
    return parser


def main() -> None:
    args = build_parser().parse_args()

    try:
        config = load_config(args.config)
        config = apply_cli_overrides(config, args)
        validate_config(config)

        dry_run = not args.execute
        print("=" * 55)
        print("Backlog 課題クローンツール")
        print("=" * 55)
        print(f"スペース  : {config['backlog']['space_host']}")
        print(
            "モード    : "
            + ("DRY RUN（実際の作成/更新は行いません）" if dry_run else "EXECUTE（Backlog に作成/更新します）")
        )
        print()

        outcome = run(args, config)
    except ConfigError as e:
        print(f"エラー: {e}", file=sys.stderr)
        sys.exit(EXIT_CONFIG_ERROR)
    except BacklogError as e:
        print(f"エラー: {e}", file=sys.stderr)
        if e.hint:
            print(f"  → {e.hint}", file=sys.stderr)
        sys.exit(EXIT_API_ERROR)

    sys.exit(exit_code_for(outcome, args.detailed_exit_code))


if __name__ == "__main__":
    main()
