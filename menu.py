"""
Backlog 課題クローン 対話メニュー
=================================
3 つの動作モードを対話形式で選び、課題キーをその場で入力して実行する。

  python3 menu.py                  # メニューを表示
  python3 menu.py --config my.yaml # 接続設定を指定

設定ファイルには接続情報（backlog セクション）だけあればよく、
複製の対象は実行のたびにこのメニューで指定する。

無人実行はこのメニューではなく本体を直接呼ぶこと:
  python3 backlog_issue_cloner.py --execute --yes
"""

import argparse
import json
import re
import subprocess
import sys
from datetime import date, timedelta
from pathlib import Path

import backlog_issue_cloner as cloner

WIDTH = 60
TOOL_DIR = Path(__file__).resolve().parent
SCRIPT = TOOL_DIR / "backlog_issue_cloner.py"
HISTORY_PATH = Path.home() / ".backlog_issue_cloner_menu.json"

# 課題キーの形式（例: PROJ-123）
ISSUE_KEY_RE = re.compile(r"^[A-Za-z][A-Za-z0-9_]*-\d+$")

MODE_SIMPLE = "simple"
MODE_PERIODIC = "periodic"
MODE_UPDATE = "update"

MODES = [
    (MODE_SIMPLE, "単純複製", "課題を子課題ごとそのままコピーする（毎回新規作成）"),
    (MODE_PERIODIC, "定期作成", "日付入りの件名で作成する。既にあれば本文だけ更新する"),
    (MODE_UPDATE, "直接更新", "既存の課題（と子課題）にコピー元の本文を反映する"),
]

DEFAULT_SUMMARY_TEMPLATE = "【定期】{YYYYMMDD} タスク"

# 終了コード（tool_launcher と揃える）
EXIT_OK = 0
EXIT_EOF = 1
EXIT_USAGE = 2
EXIT_INTERRUPTED = 130


# ===========================================================================
# 表示・入力
# ===========================================================================


def hr(char="="):
    print(char * WIDTH)


def print_menu(title: str, items: list, back_label: str = "戻る",
               default: int | None = None, default_mark: str = "既定") -> int:
    """
    メニューを表示して選択番号を返す。0 = 戻る / 終了。
    default_mark は既定値の由来を示すラベル（前回の選択なら「前回」）。
    """
    while True:
        print()
        hr()
        print(f"  {title}")
        hr()
        for i, item in enumerate(items, 1):
            mark = f" ←{default_mark}" if default == i else ""
            print(f"  {i}. {item}{mark}")
        hr("-")
        print(f"  0. {back_label}")
        hr()
        prompt = ("番号を入力してください"
                  + (f" [Enter={default}]: " if default else ": "))
        choice = input(prompt).strip()
        if not choice and default:
            return default
        if choice == "0":
            return 0
        if choice.isdigit() and 1 <= int(choice) <= len(items):
            return int(choice)
        print("  ※ 無効な入力です。もう一度入力してください。")


def input_text(prompt: str, default: str = "", validate=None,
               hint: str = "") -> str | None:
    """
    文字列を入力させる。空 Enter は default を採用する。
    default が無い状態で空 Enter を押した場合は None（キャンセル）を返す。
    """
    suffix = f" [Enter={default}]" if default else "（空 Enter で戻る）"
    while True:
        answer = input(f"  {prompt}{suffix}: ").strip()
        if not answer:
            if default:
                return default
            return None
        if validate is None or validate(answer):
            return answer
        print(f"  ※ {hint}")


def is_issue_key(value: str) -> bool:
    return bool(ISSUE_KEY_RE.match(value))


# ===========================================================================
# 前回値の記憶
# ===========================================================================


def load_history() -> dict:
    """前回の入力値を読む。壊れていても既定値で続行する。"""
    try:
        with open(HISTORY_PATH, encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, dict) else {}
    except (OSError, ValueError):
        return {}


def save_history(data: dict) -> None:
    """入力値を保存する。書けなくても実行は妨げない。"""
    try:
        with open(HISTORY_PATH, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
    except OSError:
        pass


# ===========================================================================
# 日付の選択
# ===========================================================================


def date_presets(today: date | None = None) -> list:
    """(ラベル, YYYYMMDD) の一覧を返す。"""
    today = today or date.today()
    next_monday = today + timedelta(days=(7 - today.weekday()) or 7)
    return [
        (f"今日 ({today:%Y-%m-%d})", f"{today:%Y%m%d}"),
        (f"明日 ({today + timedelta(days=1):%Y-%m-%d})",
         f"{today + timedelta(days=1):%Y%m%d}"),
        (f"来週の月曜 ({next_monday:%Y-%m-%d})", f"{next_monday:%Y%m%d}"),
    ]


def choose_date() -> str | None:
    """日付を選ぶ。戻る場合は None。"""
    presets = date_presets()
    items = [label for label, _ in presets] + ["手動入力 (YYYYMMDD)"]
    choice = print_menu("日付を選択", items, default=1)
    if choice == 0:
        return None
    if choice <= len(presets):
        return presets[choice - 1][1]
    return input_text(
        "日付を入力 (YYYYMMDD)",
        validate=lambda v: _valid_date(v),
        hint="YYYYMMDD 形式で入力してください（例: 20260401）",
    )


def _valid_date(value: str) -> bool:
    try:
        cloner.resolve_date(value)
        return True
    except cloner.ConfigError:
        return False


# ===========================================================================
# 実行内容の組み立て
# ===========================================================================


def build_args(mode: str, source_key: str, *, config_path: str = "",
               template: str = "", target_key: str = "", date_str: str = "",
               execute: bool = False) -> list:
    """本体に渡すコマンドライン引数を組み立てる。"""
    args = []
    if config_path:
        args += ["--config", config_path]
    args += ["--source-issue-key", source_key]

    if mode == MODE_SIMPLE:
        # 設定ファイルに件名テンプレートがあっても単純複製として扱う
        args.append("--no-summary-template")
    elif mode == MODE_PERIODIC:
        args += ["--summary-template", template]
        if date_str:
            args += ["--date", date_str]
    elif mode == MODE_UPDATE:
        args += ["--target-issue-key", target_key]

    if execute:
        args.append("--execute")
    return args


def run_cloner(args: list) -> int:
    """本体を実行して終了コードを返す。"""
    if not SCRIPT.is_file():
        print(f"\n  ※ 本体が見つかりません: {SCRIPT}")
        return 127
    print()
    hr("-")
    try:
        completed = subprocess.run([sys.executable, str(SCRIPT), *args], cwd=TOOL_DIR)
    except KeyboardInterrupt:
        print("\n  中断しました。")
        return EXIT_INTERRUPTED
    return completed.returncode


# ===========================================================================
# 各モードの対話
# ===========================================================================


def collect_inputs(mode: str, history: dict) -> dict | None:
    """モードに応じた入力を集める。戻る場合は None。"""
    source_key = input_text(
        "コピー元の課題キー",
        default=history.get("source_issue_key", ""),
        validate=is_issue_key,
        hint="PROJ-123 の形式で入力してください。",
    )
    if not source_key:
        return None
    result = {"source_key": source_key}

    if mode == MODE_PERIODIC:
        template = input_text(
            "件名テンプレート",
            default=history.get("summary_template") or DEFAULT_SUMMARY_TEMPLATE,
        )
        if not template:
            return None
        result["template"] = template
        if "{YYYYMMDD}" in template:
            date_str = choose_date()
            if not date_str:
                return None
            result["date_str"] = date_str

    elif mode == MODE_UPDATE:
        target_key = input_text(
            "複製先の課題キー",
            default=history.get("target_issue_key", ""),
            validate=is_issue_key,
            hint="PROJ-456 の形式で入力してください。",
        )
        if not target_key:
            return None
        if target_key.lower() == source_key.lower():
            print("  ※ コピー元と複製先が同じ課題です。")
            return None
        result["target_key"] = target_key

    return result


def run_mode(mode: str, label: str, config_path: str, history: dict) -> int | None:
    """1 つのモードを対話で実行する。戻る場合は None。"""
    print()
    hr()
    print(f"  {label}")
    hr()

    inputs = collect_inputs(mode, history)
    if inputs is None:
        return None

    choice = print_menu(
        "実行モード",
        ["ドライラン（確認のみ・変更しません）", "実行（Backlog に反映）"],
        default=1,
    )
    if choice == 0:
        return None

    # 記憶するのは入力値のみ。実行前に保存しておき、途中で失敗しても次回に活きるようにする。
    history.update({
        "mode": mode,
        "source_issue_key": inputs["source_key"],
        "summary_template": inputs.get("template", history.get("summary_template", "")),
        "target_issue_key": inputs.get("target_key", history.get("target_issue_key", "")),
    })
    save_history(history)

    args = build_args(
        mode,
        inputs["source_key"],
        config_path=config_path,
        template=inputs.get("template", ""),
        target_key=inputs.get("target_key", ""),
        date_str=inputs.get("date_str", ""),
        execute=(choice == 2),
    )
    # 実行時の確認（実行内容の一覧と y/N）は本体側が行うため、ここでは確認しない。
    return run_cloner(args)


# ===========================================================================
# エントリポイント
# ===========================================================================


def describe_connection(config_path: str) -> str:
    """接続先を 1 行で表す。読めない場合は理由を返す。"""
    try:
        config = cloner.load_config(config_path)
    except cloner.ConfigError as e:
        return f"※ {e}"
    if not isinstance(config, dict):
        return "※ 設定ファイルの内容が空です"
    host = (config.get("backlog") or {}).get("space_host")
    if not host or host == "yourcompany.backlog.com":
        return "※ backlog.space_host が未設定です"
    return str(host)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Backlog 課題クローン 対話メニュー",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="無人実行には backlog_issue_cloner.py を直接使ってください。",
    )
    parser.add_argument(
        "--config",
        default=str(TOOL_DIR / "config.yaml"),
        help="接続設定のパス（デフォルト: config.yaml）",
    )
    return parser


def main() -> None:
    args = build_parser().parse_args()
    history = load_history()
    last_mode = history.get("mode")
    default_choice = next(
        (i for i, (key, _, _) in enumerate(MODES, 1) if key == last_mode), None
    )

    items = [f"{label} — {desc}" for _, label, desc in MODES]

    try:
        while True:
            print()
            hr()
            print("  Backlog 課題クローン")
            hr()
            print(f"  接続先: {describe_connection(args.config)}")
            choice = print_menu("モードを選択", items, back_label="終了",
                                default=default_choice, default_mark="前回")
            if choice == 0:
                print("  終了します。")
                sys.exit(EXIT_OK)

            mode, label, _ = MODES[choice - 1]
            rc = run_mode(mode, label, args.config, history)
            if rc is not None:
                print()
                hr("-")
                print(f"  終了コード: {rc}")
                default_choice = choice
                input("\n  Enter キーでメニューに戻ります...")
    except EOFError:
        print("\n  入力が終了しました。")
        sys.exit(EXIT_EOF)
    except KeyboardInterrupt:
        print("\n  中断しました。")
        sys.exit(EXIT_INTERRUPTED)


if __name__ == "__main__":
    main()
