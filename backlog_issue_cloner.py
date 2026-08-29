"""
Backlog 課題クローンツール
==========================
指定した課題の description をコピーして新規課題を作成する CLI ツール。

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

# backlog セクションで受け付ける数値設定と、その最小値
NUMERIC_SETTINGS = (
    ("timeout", 1),
    ("max_retries", 0),
    ("retry_backoff", 0),
    ("retry_max_delay", 0),
)


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

    def _build_query(self, params: dict) -> str:
        """パラメータ dict をクエリ文字列に変換（リスト値は [] 展開）"""
        parts = []
        for key, value in params.items():
            if isinstance(value, list):
                for v in value:
                    parts.append(
                        f"{urllib.parse.quote(f'{key}[]')}={urllib.parse.quote(str(v))}"
                    )
            else:
                parts.append(
                    f"{urllib.parse.quote(str(key))}={urllib.parse.quote(str(value))}"
                )
        return "&".join(parts)

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

        hints = {
            400: "リクエストパラメータを確認してください。",
            401: "api_key を確認してください。",
            403: "api_key の権限を確認してください。",
            404: "space_host または project_key を確認してください。",
            429: "レート制限に達しました。しばらく待って再実行してください。",
        }
        raise BacklogError(message, status=e.code, hint=hints.get(e.code))

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

    def _request(
        self,
        req: urllib.request.Request,
        endpoint: str,
        *,
        allow_404: bool = False,
        raise_no_change: bool = False,
    ):
        """
        リクエストを送信し、JSON をデコードして返す。
        429 / 5xx / ネットワークエラーは max_retries 回まで指数バックオフで再試行する。
        allow_404 が True なら 404 時に None を返す。
        """
        for attempt in range(self.max_retries + 1):
            try:
                with urllib.request.urlopen(
                    req, timeout=self.timeout, context=self.ssl_context
                ) as res:
                    return json.loads(res.read().decode("utf-8"))
            except urllib.error.HTTPError as e:
                # HTTPError は URLError のサブクラスなので必ず先に捕捉する
                if allow_404 and e.code == 404:
                    _close_quietly(e)
                    return None
                if e.code in RETRYABLE_STATUS and attempt < self.max_retries:
                    delay = self._retry_delay(e, attempt)
                    _close_quietly(e)
                    print(
                        f"  警告: HTTP {e.code}（{endpoint}）。"
                        f"{delay:.1f} 秒後に再試行します"
                        f"（{attempt + 1}/{self.max_retries}）",
                        file=sys.stderr,
                    )
                    time.sleep(delay)
                    continue
                self._handle_http_error(e, endpoint, raise_no_change=raise_no_change)
            except (urllib.error.URLError, TimeoutError) as e:
                # 接続時のエラーは URLError に包まれるが、レスポンス待ちや読み込み中の
                # タイムアウトは TimeoutError のまま送出されるため両方を捕捉する。
                reason = getattr(e, "reason", None) or e
                if attempt < self.max_retries:
                    delay = self._retry_delay(None, attempt)
                    print(
                        f"  警告: 接続エラー（{endpoint}）: {reason}。"
                        f"{delay:.1f} 秒後に再試行します"
                        f"（{attempt + 1}/{self.max_retries}）",
                        file=sys.stderr,
                    )
                    time.sleep(delay)
                    continue
                raise BacklogError(
                    f"ネットワークエラー（{endpoint}）: {reason}",
                    hint="space_host とネットワーク接続を確認してください。",
                ) from e

    def _get(
        self, endpoint: str, params: dict = None, *, allow_404: bool = False
    ) -> dict | list | None:
        """GET リクエストを送信する。allow_404 が True なら 404 時に None を返す。"""
        params = dict(params or {})
        params["apiKey"] = self.api_key
        query = self._build_query(params)
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

        body_parts = []
        for key, value in params.items():
            if isinstance(value, list):
                for v in value:
                    body_parts.append((f"{key}[]", str(v)))
            else:
                body_parts.append((key, str(value)))

        body = "&".join(
            f"{k}={urllib.parse.quote_plus(v)}"
            for k, v in body_parts
        ).encode("utf-8")

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
        return self._request(req, endpoint, raise_no_change=raise_no_change)

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

    def search_issues_by_keyword(
        self, project_id: int, keyword: str, status_ids: list | None = None
    ):
        """
        keyword でプロジェクト内の課題を遅延列挙する（ページネーション対応）。
        ジェネレータのため、呼び出し元が途中で打ち切れば以降のページは取得しない。
        status_ids を渡すとその状態の課題のみに絞り込む。
        Backlog の keyword 検索は summary + description を対象とするため、
        呼び出し元で summary のフィルタを行うこと。
        """
        offset = 0
        count = 100
        while True:
            params = {
                "projectId": [project_id],
                "keyword": keyword,
                "count": count,
                "offset": offset,
            }
            if status_ids:
                params["statusId"] = list(status_ids)
            issues = self._get("/issues", params)
            if not issues:
                return
            yield from issues
            if len(issues) < count:
                return
            offset += count
            time.sleep(0.3)

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

    c = _require_section(config, "clone")
    src = c.get("source_issue_key", "")
    if not src or src == "PROJ-123":
        raise ConfigError("config.yaml の clone.source_issue_key を設定してください。")
    if not c.get("summary_template"):
        raise ConfigError("config.yaml の clone.summary_template を設定してください。")

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

    # 真偽値設定: "false" のような文字列を真と誤解しないよう型を確認する
    for section_name, section, key in (
        ("backlog", b, "ssl_verify"),
        ("clone", c, "include_closed"),
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


def resolve_issue_type_id(
    client: BacklogClient, project_key: str, name: str | None
) -> tuple[int, str]:
    """種別IDと種別名を返す。見つからない場合は警告して最初の種別にフォールバック。"""
    types = client.get_issue_types(project_key)
    if not types:
        raise ConfigError(f"プロジェクト {project_key} の種別が取得できませんでした。")
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


def resolve_priority_id(
    client: BacklogClient, name: str | None
) -> tuple[int, str]:
    """優先度IDと優先度名を返す。見つからない場合は「中」→ 最初の優先度にフォールバック。"""
    priorities = client.get_priorities()
    if not priorities:
        raise ConfigError("優先度一覧が取得できませんでした。")
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
) -> dict | None:
    """
    件名が summary にマッチする課題を返す。なければ None。
    keyword 検索は summary + description を対象とするため、件名側でフィルタする。
    match_mode="exact" なら完全一致、"substring" なら部分一致。
    status_ids を渡すとその状態の課題のみを検索対象にする。
    検索結果は遅延列挙されるため、最初にマッチした時点で以降のページは取得しない。
    """
    for issue in client.search_issues_by_keyword(project_id, summary, status_ids):
        candidate = issue.get("summary", "")
        if match_mode == "exact":
            if candidate == summary:
                return issue
        elif summary in candidate:
            return issue
    return None


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


def confirm_create(
    summary: str, source_key: str, description_preview: str, assume_yes: bool = False
) -> bool:
    print("\n新規作成の確認:")
    print(f"  件名      : {summary}")
    print(f"  コピー元  : {source_key}")
    preview = description_preview[:200]
    if preview:
        print(f"  本文冒頭  : {preview!r}")
    return _ask("  Backlog に新規作成しますか？ [y/N]: ", assume_yes)


def confirm_update(
    existing_key: str, existing_desc: str, source_desc: str, assume_yes: bool = False
) -> bool:
    print(f"\n本文更新の確認 ({existing_key}):")
    print(f"  既存の本文（冒頭）: {existing_desc[:120]!r}")
    print(f"  新しい本文（冒頭）: {source_desc[:120]!r}")
    return _ask("  既存課題の本文を更新しますか？ [y/N]: ", assume_yes)


# ===========================================================================
# メインロジック
# ===========================================================================


def run(args: argparse.Namespace, config: dict) -> str:
    """クローン処理を実行し、OUTCOME_* のいずれかを返す。"""
    dry_run = not args.execute
    assume_yes = bool(getattr(args, "yes", False))
    backlog_cfg = config["backlog"]
    clone_cfg = config["clone"]

    # 1. 日付解決 → 件名テンプレート展開
    date_str = resolve_date(args.date)
    summary = clone_cfg["summary_template"].replace("{YYYYMMDD}", date_str)

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

    # 4. 対象プロジェクトのキーと ID を確定
    # Backlog API の単一課題レスポンスには projectId（数値）のみ含まれ project オブジェクトはない。
    # コピー元と同じプロジェクトなら issueKey（例: PROJ-123）のプレフィックスをキーとし、
    # ID は取得済みの source_issue["projectId"] を流用して API 呼び出しを 1 回節約する。
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

    # 5. issueTypeId / priorityId を解決
    issue_type_id, issue_type_name = resolve_issue_type_id(
        client, target_project_key, clone_cfg.get("issue_type")
    )
    priority_id, priority_name = resolve_priority_id(
        client, clone_cfg.get("priority")
    )

    # 6. 重複検出の条件を確定
    match_mode = clone_cfg.get("match_mode", "substring")
    include_closed = bool(clone_cfg.get("include_closed", False))
    status_ids = None if include_closed else STATUS_IDS_OPEN

    # 7. 解決済み設定値を表示
    prefix = "[DRY RUN] " if dry_run else ""
    print(f"\n{prefix}設定値:")
    print(f"  件名        : {summary}")
    print(f"  コピー元    : {source_key}")
    print(f"  対象PJ      : {target_project_key} (id={project_id})")
    print(f"  種別        : {issue_type_name} (id={issue_type_id})")
    print(f"  優先度      : {priority_name} (id={priority_id})")
    print(f"  本文文字数  : {len(source_desc)} 文字")
    print(f"  重複判定    : {match_mode}"
          f"（完了済み課題を{'含む' if include_closed else '除く'}）")

    # 8. 重複チェック
    print(f"\n既存課題を検索中（件名: {summary!r}）...")
    existing = find_existing_by_summary(
        client, project_id, summary, match_mode=match_mode, status_ids=status_ids
    )

    if existing:
        existing_key = existing["issueKey"]
        existing_desc = existing.get("description") or ""

        if existing_desc == source_desc:
            # 8a. description も同じ → 何もしない
            print(f"既存課題あり、変更なし: {existing_key}")
            return OUTCOME_NO_CHANGE

        # 8b. description に差分あり → 更新フロー
        print(f"既存課題あり、本文に差分あり: {existing_key}")
        if dry_run:
            print(f"[DRY RUN] 本文を更新します: {existing_key}")
            print(f"  既存本文（冒頭）: {existing_desc[:120]!r}")
            print(f"  新規本文（冒頭）: {source_desc[:120]!r}")
            return OUTCOME_UPDATED

        if not confirm_update(existing_key, existing_desc, source_desc, assume_yes):
            print(f"スキップ（更新をキャンセル）: {existing_key}")
            return OUTCOME_SKIPPED

        try:
            updated = client.update_issue(existing_key, {"description": source_desc})
        except BacklogNoChangeError:
            print(f"スキップ（変更なし）: {existing_key}")
            return OUTCOME_NO_CHANGE
        print(f"更新完了: {updated['issueKey']} — {updated['summary']}")
        return OUTCOME_UPDATED

    # 9. 既存課題なし → 新規作成フロー
    if dry_run:
        print("[DRY RUN] 新規課題を作成します:")
        print(f"  件名: {summary}")
        if source_desc:
            print(f"  本文（冒頭）: {source_desc[:200]!r}")
        return OUTCOME_CREATED

    if not confirm_create(summary, source_key, source_desc, assume_yes):
        print("スキップ（作成をキャンセル）")
        return OUTCOME_SKIPPED

    params = {
        "projectId": project_id,
        "summary": summary,
        "issueTypeId": issue_type_id,
        "priorityId": priority_id,
        "description": source_desc,
    }
    created = client.create_issue(params)
    print(f"作成完了: {created['issueKey']} — {created['summary']}")
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
    return parser


def main() -> None:
    args = build_parser().parse_args()

    try:
        config = load_config(args.config)
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
