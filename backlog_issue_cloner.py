"""
Backlog 課題クローンツール
==========================
指定した課題の description をコピーして新規課題を作成する CLI ツール。

使い方:
  python backlog_issue_cloner.py                   # ドライラン（デフォルト）
  python backlog_issue_cloner.py --execute         # 実際に作成/更新
  python backlog_issue_cloner.py --date 20260401   # 日付を指定
  python backlog_issue_cloner.py --execute --debug # デバッグ出力付き
  python backlog_issue_cloner.py --config my.yaml  # 設定ファイルを指定

依存:
  pip install pyyaml
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
# Backlog API クライアント
# (excel_to_backlog/backlog_client.py をベースに必要なメソッドのみ抽出)
# ===========================================================================


class BacklogNoChangeError(Exception):
    """更新内容が現在の課題と同一のため変更なしと判断されたエラー。"""


class BacklogClient:
    def __init__(
        self,
        space_host: str,
        api_key: str,
        ssl_verify: bool = True,
        base_path: str = "",
        debug: bool = False,
    ):
        base_path = "/" + base_path.strip("/") if base_path.strip("/") else ""
        self.base_url = f"https://{space_host}{base_path}/api/v2"
        self.api_key = api_key
        self.debug = debug

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
                        f"{urllib.parse.quote(str(key))}%5B%5D={urllib.parse.quote(str(v))}"
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

        if raise_no_change and e.code == 400 and any(
            err.get("code") == 7 for err in errors
        ):
            raise BacklogNoChangeError(detail or "HTTP 400 / code 7（変更なしと判断）")

        print(
            f"エラー: API呼び出しに失敗しました（HTTP {e.code}）: {endpoint}",
            file=sys.stderr,
        )
        if detail:
            print(f"  詳細: {detail}", file=sys.stderr)
        elif raw_body:
            print(f"  レスポンス: {raw_body[:500]}", file=sys.stderr)

        hints = {
            400: "リクエストパラメータを確認してください。",
            401: "api_key を確認してください。",
            403: "api_key の権限を確認してください。",
            404: "space_host または project_key を確認してください。",
        }
        if e.code in hints:
            print(f"  → {hints[e.code]}", file=sys.stderr)
        sys.exit(1)

    def _get(self, endpoint: str, params: dict = None) -> dict | list:
        params = dict(params or {})
        params["apiKey"] = self.api_key
        query = self._build_query(params)
        url = f"{self.base_url}{endpoint}?{query}"

        if self.debug:
            debug_parts = [p for p in query.split("&") if not p.startswith("apiKey=")]
            print(f"  [DEBUG GET] {endpoint} ?" + "&".join(debug_parts), file=sys.stderr)

        req = urllib.request.Request(url)
        try:
            with urllib.request.urlopen(req, timeout=30, context=self.ssl_context) as res:
                return json.loads(res.read().decode("utf-8"))
        except urllib.error.HTTPError as e:
            self._handle_http_error(e, endpoint)

    def _post(self, endpoint: str, params: dict) -> dict:
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
            print(f"  [DEBUG POST] {endpoint}", file=sys.stderr)
            for k, v in body_parts:
                print(f"    {k}={v}", file=sys.stderr)

        req = urllib.request.Request(
            url,
            data=body,
            method="POST",
            headers={"Content-Type": "application/x-www-form-urlencoded"},
        )
        try:
            with urllib.request.urlopen(req, timeout=30, context=self.ssl_context) as res:
                return json.loads(res.read().decode("utf-8"))
        except urllib.error.HTTPError as e:
            self._handle_http_error(e, endpoint)

    def _patch(self, endpoint: str, params: dict, *, raise_no_change: bool = False) -> dict:
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
            print(f"  [DEBUG PATCH] {endpoint}", file=sys.stderr)
            for k, v in body_parts:
                print(f"    {k}={v}", file=sys.stderr)

        req = urllib.request.Request(
            url,
            data=body,
            method="PATCH",
            headers={"Content-Type": "application/x-www-form-urlencoded"},
        )
        try:
            with urllib.request.urlopen(req, timeout=30, context=self.ssl_context) as res:
                return json.loads(res.read().decode("utf-8"))
        except urllib.error.HTTPError as e:
            self._handle_http_error(e, endpoint, raise_no_change=raise_no_change)

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
        url = (
            f"{self.base_url}/issues/{urllib.parse.quote(str(issue_id_or_key))}"
            f"?apiKey={urllib.parse.quote(self.api_key)}"
        )
        req = urllib.request.Request(url)
        try:
            with urllib.request.urlopen(req, timeout=30, context=self.ssl_context) as res:
                return json.loads(res.read().decode("utf-8"))
        except urllib.error.HTTPError as e:
            if e.code == 404:
                return None
            self._handle_http_error(e, f"/issues/{issue_id_or_key}")

    def search_issues_by_keyword(self, project_id: int, keyword: str) -> list:
        """
        keyword でプロジェクト内の課題を検索（ページネーション対応）。
        Backlog の keyword 検索は summary + description を対象とするため、
        呼び出し元で summary の完全一致フィルタを行うこと。
        """
        all_issues = []
        offset = 0
        count = 100
        while True:
            issues = self._get("/issues", {
                "projectId": [project_id],
                "keyword": keyword,
                "count": count,
                "offset": offset,
            })
            if not issues:
                break
            all_issues.extend(issues)
            if len(issues) < count:
                break
            offset += count
            time.sleep(0.3)
        return all_issues

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
        print(f"エラー: 設定ファイルが見つかりません: {config_path}", file=sys.stderr)
        sys.exit(1)
    with open(path, encoding="utf-8") as f:
        return yaml.safe_load(f)


def validate_config(config: dict) -> None:
    b = config.get("backlog", {})
    for key, placeholder in [
        ("space_host", "yourcompany.backlog.com"),
        ("api_key", "YOUR_API_KEY_HERE"),
    ]:
        val = b.get(key, "")
        if not val or val == placeholder:
            print(f"エラー: config.yaml の backlog.{key} を設定してください。", file=sys.stderr)
            sys.exit(1)

    c = config.get("clone", {})
    src = c.get("source_issue_key", "")
    if not src or src == "PROJ-123":
        print("エラー: config.yaml の clone.source_issue_key を設定してください。", file=sys.stderr)
        sys.exit(1)
    if not c.get("summary_template"):
        print("エラー: config.yaml の clone.summary_template を設定してください。", file=sys.stderr)
        sys.exit(1)


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
            print(
                f"エラー: --date の形式が不正です（YYYYMMDD 形式で指定してください）: {date_arg}",
                file=sys.stderr,
            )
            sys.exit(1)
    return datetime.now().strftime("%Y%m%d")


def resolve_issue_type_id(
    client: BacklogClient, project_key: str, name: str | None
) -> tuple[int, str]:
    """種別IDと種別名を返す。見つからない場合は警告して最初の種別にフォールバック。"""
    types = client.get_issue_types(project_key)
    if not types:
        print(f"エラー: プロジェクト {project_key} の種別が取得できませんでした。", file=sys.stderr)
        sys.exit(1)
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
        print("エラー: 優先度一覧が取得できませんでした。", file=sys.stderr)
        sys.exit(1)
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
    client: BacklogClient, project_id: int, summary: str
) -> dict | None:
    """
    summary と完全一致する課題を返す。なければ None。
    keyword 検索は summary + description を対象とするため完全一致フィルタが必須。
    """
    candidates = client.search_issues_by_keyword(project_id, summary)
    for issue in candidates:
        if issue.get("summary", "") == summary:
            return issue
    return None


# ===========================================================================
# 確認プロンプト
# ===========================================================================


def confirm_create(summary: str, source_key: str, description_preview: str) -> bool:
    print("\n新規作成の確認:")
    print(f"  件名      : {summary}")
    print(f"  コピー元  : {source_key}")
    preview = description_preview[:200]
    if preview:
        print(f"  本文冒頭  : {preview!r}")
    try:
        answer = input("  Backlog に新規作成しますか？ [y/N]: ").strip().lower()
    except EOFError:
        answer = ""
    return answer in ("y", "yes")


def confirm_update(existing_key: str, existing_desc: str, source_desc: str) -> bool:
    print(f"\n本文更新の確認 ({existing_key}):")
    print(f"  既存の本文（冒頭）: {existing_desc[:120]!r}")
    print(f"  新しい本文（冒頭）: {source_desc[:120]!r}")
    try:
        answer = input("  既存課題の本文を更新しますか？ [y/N]: ").strip().lower()
    except EOFError:
        answer = ""
    return answer in ("y", "yes")


# ===========================================================================
# メインロジック
# ===========================================================================


def run(args: argparse.Namespace, config: dict) -> None:
    dry_run = not args.execute
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
    )

    # 3. コピー元課題を取得
    source_key = clone_cfg["source_issue_key"]
    print(f"コピー元課題を取得中: {source_key}")
    source_issue = client.get_issue(source_key)
    if source_issue is None:
        print(f"エラー: コピー元課題「{source_key}」が見つかりません。", file=sys.stderr)
        sys.exit(1)
    source_desc = source_issue.get("description") or ""

    # 4. 対象プロジェクトキーを確定
    # Backlog API の単一課題レスポンスには projectId（数値）のみ含まれ project オブジェクトはない。
    # issueKey（例: PROJ-123）のプレフィックスをプロジェクトキーとして使用する。
    target_project_key = (
        clone_cfg.get("target_project_key")
        or source_issue["issueKey"].rsplit("-", 1)[0]
    )

    # 5. プロジェクト情報取得（ID解決）
    print(f"対象プロジェクトを取得中: {target_project_key}")
    project = client.get_project(target_project_key)
    project_id = project["id"]

    # 6. issueTypeId / priorityId を解決
    issue_type_id, issue_type_name = resolve_issue_type_id(
        client, target_project_key, clone_cfg.get("issue_type")
    )
    priority_id, priority_name = resolve_priority_id(
        client, clone_cfg.get("priority")
    )

    # 7. 解決済み設定値を表示
    prefix = "[DRY RUN] " if dry_run else ""
    print(f"\n{prefix}設定値:")
    print(f"  件名        : {summary}")
    print(f"  コピー元    : {source_key}")
    print(f"  対象PJ      : {target_project_key} (id={project_id})")
    print(f"  種別        : {issue_type_name} (id={issue_type_id})")
    print(f"  優先度      : {priority_name} (id={priority_id})")
    print(f"  本文文字数  : {len(source_desc)} 文字")

    # 8. 重複チェック
    print(f"\n既存課題を検索中（件名: {summary!r}）...")
    existing = find_existing_by_summary(client, project_id, summary)

    if existing:
        existing_key = existing["issueKey"]
        existing_desc = existing.get("description") or ""

        if existing_desc == source_desc:
            # 8a. description も同じ → 何もしない
            print(f"既存課題あり、変更なし: {existing_key}")
            return

        # 8b. description に差分あり → 更新フロー
        print(f"既存課題あり、本文に差分あり: {existing_key}")
        if dry_run:
            print(f"[DRY RUN] 本文を更新します: {existing_key}")
            print(f"  既存本文（冒頭）: {existing_desc[:120]!r}")
            print(f"  新規本文（冒頭）: {source_desc[:120]!r}")
            return

        if not confirm_update(existing_key, existing_desc, source_desc):
            print(f"スキップ（更新をキャンセル）: {existing_key}")
            return

        try:
            updated = client.update_issue(existing_key, {"description": source_desc})
            print(f"更新完了: {updated['issueKey']} — {updated['summary']}")
        except BacklogNoChangeError:
            print(f"スキップ（変更なし）: {existing_key}")
        return

    # 9. 既存課題なし → 新規作成フロー
    if dry_run:
        print("[DRY RUN] 新規課題を作成します:")
        print(f"  件名: {summary}")
        if source_desc:
            print(f"  本文（冒頭）: {source_desc[:200]!r}")
        return

    if not confirm_create(summary, source_key, source_desc):
        print("スキップ（作成をキャンセル）")
        return

    params = {
        "projectId": project_id,
        "summary": summary,
        "issueTypeId": issue_type_id,
        "priorityId": priority_id,
        "description": source_desc,
    }
    created = client.create_issue(params)
    print(f"作成完了: {created['issueKey']} — {created['summary']}")


# ===========================================================================
# エントリポイント
# ===========================================================================


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Backlog 課題クローンツール",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
例:
  python backlog_issue_cloner.py                          # ドライラン（デフォルト）
  python backlog_issue_cloner.py --execute                # 実際に作成/更新
  python backlog_issue_cloner.py --date 20260401          # 日付を指定
  python backlog_issue_cloner.py --execute --debug        # デバッグ出力付きで実行
  python backlog_issue_cloner.py --config my_config.yaml  # 設定ファイルを指定
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
        "--debug",
        action="store_true",
        help="API リクエストの詳細を表示する",
    )
    args = parser.parse_args()

    config = load_config(args.config)
    validate_config(config)

    dry_run = not args.execute
    print("=" * 55)
    print("Backlog 課題クローンツール")
    print("=" * 55)
    print(f"スペース  : {config['backlog']['space_host']}")
    print(
        f"モード    : "
        + ("DRY RUN（実際の作成/更新は行いません）" if dry_run else "EXECUTE（Backlog に作成/更新します）")
    )
    print()

    run(args, config)


if __name__ == "__main__":
    main()
