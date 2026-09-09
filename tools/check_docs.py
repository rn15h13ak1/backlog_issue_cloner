"""
ドキュメントと実装の整合を検査する
====================================
設定項目・コマンドラインオプション・終了コードが、実装と README と
config.sample.yaml で一致しているかを確認する。リンク切れも検出する。

    python3 tools/check_docs.py

不一致があれば内容を表示して終了コード 1 を返す。

実装を変えたのにドキュメントを直し忘れる、という取りこぼしを防ぐためのもの。
「なぜそうなっているか」の記述までは検査できないので、docs/DESIGN.md の
設計判断は人が読んで確認すること。
"""

import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

import backlog_issue_cloner as cloner  # noqa: E402

# 廃止した設定項目。実装は検出してエラーにするだけで、値としては使わない。
DEPRECATED_KEYS = {"target_project_key"}

# 実装が既定値を持つため .get() に現れない設定項目
IMPLICIT_KEYS = {
    "backlog": {"ssl_verify"},
    "clone": {"include_closed", "include_children",
              "copy_custom_fields", "copy_attributes"},
}


def read(name: str) -> str:
    return (ROOT / name).read_text(encoding="utf-8")


def config_keys_in_code(src: str) -> dict:
    """実装が参照している設定キーをセクションごとに集める。"""
    found = {"backlog": set(), "clone": set()}
    for var, section in (("backlog_cfg", "backlog"), ("clone_cfg", "clone"),
                         ("b", "backlog"), ("c", "clone")):
        found[section] |= set(re.findall(rf'{var}\.get\(\s*"([^"]+)"', src))
        found[section] |= set(re.findall(rf'{var}\[\s*"([^"]+)"\s*\]', src))
    found["backlog"] |= {k for k, _ in cloner.NUMERIC_SETTINGS}
    for section, keys in IMPLICIT_KEYS.items():
        found[section] |= keys
    for section in found:
        found[section] -= DEPRECATED_KEYS
    return found


def config_keys_in_readme(readme: str, section: str) -> set:
    marker = f"### `{section}` セクション"
    if marker not in readme:
        return set()
    body = readme.split(marker)[1].split("\n##")[0]
    return set(re.findall(r"^\| `([a-z_]+)`", body, re.M))


def config_keys_in_sample(sample: str) -> set:
    return set(re.findall(r"^\s*#?\s*([a-z_]+):", sample, re.M)) - {"backlog", "clone"}


def cli_options(src: str) -> set:
    return {m[1] for m in
            re.findall(r'add_argument\(\s*(?:"(-\w)",\s*)?"(--[\w-]+)"', src)}


def anchors(markdown: str) -> set:
    """GitHub の見出しアンカーを再現する。"""
    return {
        re.sub(r"[^0-9a-z぀-ヿ一-鿿 -]", "", h.lower()).replace(" ", "-")
        for h in re.findall(r"^#{1,6} (.+)$", markdown, re.M)
    }


def check() -> list:
    """不一致の説明を並べて返す。空なら問題なし。"""
    problems = []
    src = read("backlog_issue_cloner.py")
    menu_src = read("menu.py")
    readme = read("README.md")
    sample = read("config.sample.yaml")
    design = read("docs/DESIGN.md")

    # 1. 設定項目
    in_code = config_keys_in_code(src)
    sample_keys = config_keys_in_sample(sample)
    for section in ("backlog", "clone"):
        code, doc = in_code[section], config_keys_in_readme(readme, section)
        for key in sorted(code - doc):
            problems.append(f"設定 {section}.{key} が README の表にありません")
        for key in sorted(doc - code):
            problems.append(f"README の表にある {section}.{key} を実装が読んでいません")
        for key in sorted(code - sample_keys):
            problems.append(f"設定 {section}.{key} が config.sample.yaml にありません")

    # 2. 廃止した設定項目が使えるものとして書かれていないか
    for key in DEPRECATED_KEYS:
        for section in ("backlog", "clone"):
            if key in config_keys_in_readme(readme, section):
                problems.append(f"廃止した {key} が README の設定項目表に残っています")

    # 3. コマンドラインオプション
    documented = set(re.findall(r"`(--[\w-]+)", readme))
    for opt in sorted(cli_options(src) - documented):
        problems.append(f"オプション {opt} が README にありません")
    for opt in sorted(cli_options(menu_src) - documented):
        problems.append(f"menu.py のオプション {opt} が README にありません")

    # 4. 終了コード
    for name in dir(cloner):
        if not name.startswith("EXIT_"):
            continue
        code = getattr(cloner, name)
        if not re.search(rf"\| `{code}` \|", readme):
            problems.append(f"終了コード {code}（{name}）が README の表にありません")

    # 5. リンクとアンカー
    for name, text in (("README.md", readme), ("docs/DESIGN.md", design)):
        found = anchors(text)
        for anchor in sorted(set(re.findall(r"\]\(#([^)]+)\)", text)) - found):
            problems.append(f"{name} のリンク #{anchor} に対応する見出しがありません")
        base = (ROOT / name).parent
        for rel in re.findall(r"\]\((?!https?:|#)([^)#]+)\)", text):
            if not (base / rel).exists():
                problems.append(f"{name} のリンク {rel} のファイルがありません")

    # 6. 仕様書が挙げる関数が実在するか
    for func in sorted(set(re.findall(r"`(\w+)\(\)`", design))):
        if func not in src and func not in menu_src:
            problems.append(f"docs/DESIGN.md が挙げる {func}() が実装にありません")

    return problems


def main() -> None:
    problems = check()
    if problems:
        print(f"ドキュメントと実装の不一致が {len(problems)} 件あります:\n")
        for p in problems:
            print(f"  - {p}")
        sys.exit(1)
    print("ドキュメントと実装は一致しています。")


if __name__ == "__main__":
    main()
