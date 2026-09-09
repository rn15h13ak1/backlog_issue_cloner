"""
テストが退行を検知できるかを測る（ミューテーションテスト）
==========================================================
実装にわざとバグを埋め込み、テストが落ちるかを確認する。

    python3 tools/mutation_test.py            # 全件実行
    python3 tools/mutation_test.py --list     # 変異の一覧だけ表示

見逃しが 1 件でもあれば終了コード 1 を返す。

カバレッジ率は「その行を通ったか」しか表さず、
「壊したときに気づけるか」は測れない。この文書化されない差を埋めるためのもの。

【再現】が付いた変異は、過去に実際に起きた不具合を再現している。
実装を直したら、その不具合を再現する変異をここに追加すること。
"""

import argparse
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent

# (説明, 対象ファイル, 置換前, 置換後)
MUTATIONS = [
    ("【再現】親の種別をコピー元から引き継がない", "backlog_issue_cloner.py",
     '    source_name = ((source_issue or {}).get("issueType") or {}).get("name")',
     '    source_name = None'),
    ("【再現】親の優先度をコピー元から引き継がない", "backlog_issue_cloner.py",
     '    source_id = ((source_issue or {}).get("priority") or {}).get("id")',
     '    source_id = None'),
    ("【再現】カスタム属性を送らない", "backlog_issue_cloner.py",
     'def custom_field_params(issue: dict) -> dict:\n    """',
     'def custom_field_params(issue: dict) -> dict:\n    return {}\n    """'),
    ("【再現】子課題を件名順に並べない", "backlog_issue_cloner.py",
     '    return sorted(\n        issues,\n'
     '        key=lambda i: (summary_sort_key(i.get("summary", "")), i.get("id") or 0),\n    )',
     '    return list(issues)'),
    ("【再現】件名の数字を文字列として比較する", "backlog_issue_cloner.py",
     '        (0, int(part), "") if part.isdigit() else (1, 0, part)',
     '        (1, 0, part)'),
    ("【再現】課題キーの先頭を英字に限定する", "menu.py",
     r'ISSUE_KEY_RE = re.compile(r"^[A-Za-z0-9_]+-\d+$")',
     r'ISSUE_KEY_RE = re.compile(r"^[A-Za-z][A-Za-z0-9_]*-\d+$")'),
    ("【再現】全角幅を考慮せず桁を揃える", "backlog_issue_cloner.py",
     '    return text + " " * max(0, width - display_width(text))',
     '    return text + " " * max(0, width - len(text))'),
    ("担当者などの属性を送らない", "backlog_issue_cloner.py",
     'def inherited_issue_params(issue: dict) -> dict:\n    """',
     'def inherited_issue_params(issue: dict) -> dict:\n    return {}\n    """'),
    ("プロジェクト跨ぎの拒否を外す", "backlog_issue_cloner.py",
     '        if existing_project_key != target_project_key:',
     '        if False:'),
    ("廃止した target_project_key を黙って無視する", "backlog_issue_cloner.py",
     '    if "target_project_key" in c:',
     '    if False:'),
    ("子課題に parentIssueId を渡さない", "backlog_issue_cloner.py",
     '        "parentIssueId": parent_id,\n        **extra,',
     '        **extra,'),
    ("重複検索でコピー元自身を除外しない", "backlog_issue_cloner.py",
     '        if exclude_id is not None and issue.get("id") == exclude_id:',
     '        if False:'),
    ("更新系でも 5xx を再試行する（二重作成の危険）", "backlog_issue_cloner.py",
     '                retryable = e.code in RETRYABLE_STATUS and (idempotent or e.code == 429)',
     '                retryable = e.code in RETRYABLE_STATUS'),
    ("exact 指定でも部分一致にする", "backlog_issue_cloner.py",
     '    if match_mode == "exact":\n        return candidate == wanted',
     '    if False:\n        return candidate == wanted'),
    ("--yes を無視する", "backlog_issue_cloner.py",
     '    if assume_yes:\n        print(f"{prompt} y（--yes 指定）")\n        return True',
     '    if False:\n        return True'),
    ("include_closed を無視して常に全状態を検索", "backlog_issue_cloner.py",
     '    status_ids = None if include_closed else STATUS_IDS_OPEN',
     '    status_ids = None'),
    ("スキップでも終了コード 0 を返す", "backlog_issue_cloner.py",
     '    if outcome == OUTCOME_SKIPPED:\n        return EXIT_SKIPPED',
     '    if False:\n        return EXIT_SKIPPED'),
    ("日付を含むテンプレートでも日付を尋ねない", "menu.py",
     '        if "{YYYYMMDD}" in template:',
     '        if False:'),
    ("複製先の未照合を警告しない", "backlog_issue_cloner.py",
     '    if unmatched_children and any(p.action == OUTCOME_CREATED for p in child_plans):',
     '    if False:'),
    ("既存子課題を重複して割り当てる", "backlog_issue_cloner.py",
     '            if i in used:\n                continue',
     '            if False:\n                continue'),
]

IGNORE = shutil.ignore_patterns(".git", "__pycache__", ".coverage",
                                "htmlcov", "config.yaml", ".venv")


def run_mutation(label: str, fname: str, old: str, new: str) -> str:
    """変異を適用してテストを実行し、"caught" / "survived" / "invalid" を返す。"""
    with tempfile.TemporaryDirectory() as tmp:
        work = Path(tmp) / "work"
        shutil.copytree(ROOT, work, ignore=IGNORE)
        target = work / fname
        source = target.read_text(encoding="utf-8")
        if old not in source:
            return "invalid"
        target.write_text(source.replace(old, new, 1), encoding="utf-8")
        result = subprocess.run(
            [sys.executable, "-m", "unittest", "discover", "-s", "tests", "-t", "."],
            cwd=work, capture_output=True, text=True,
        )
        if result.returncode == 0:
            return "survived"
        failures = result.stderr.count("FAIL:") + result.stderr.count("ERROR:")
        return f"caught:{failures}"


def main() -> None:
    parser = argparse.ArgumentParser(description="ミューテーションテスト")
    parser.add_argument("--list", action="store_true", help="変異の一覧だけ表示する")
    args = parser.parse_args()

    if args.list:
        for label, fname, _, _ in MUTATIONS:
            print(f"  {fname:26} {label}")
        print(f"\n計 {len(MUTATIONS)} 件")
        return

    caught = survived = invalid = 0
    for label, fname, old, new in MUTATIONS:
        outcome = run_mutation(label, fname, old, new)
        if outcome == "invalid":
            # 実装が変わって置換できなくなった状態。変異の定義を直す必要がある。
            print(f"  ?? 適用不可         {label}")
            invalid += 1
        elif outcome == "survived":
            print(f"  ★ 見逃し            {label}")
            survived += 1
        else:
            print(f"  ✓ 検知 ({outcome.split(':')[1]:>2}件失敗)  {label}")
            caught += 1

    total = caught + survived
    rate = f"{caught * 100 // total}%" if total else "-"
    print(f"\n検知 {caught} / 見逃し {survived} / 適用不可 {invalid}   検知率 {rate}")

    if survived or invalid:
        if survived:
            print("\n見逃した変異があります。テストを追加してください。")
        if invalid:
            print("\n適用できない変異があります。実装の変更に合わせて定義を直してください。")
        sys.exit(1)


if __name__ == "__main__":
    main()
