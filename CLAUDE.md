# backlog_issue_cloner

共通規約: [../ws-conventions/README.md](../ws-conventions/README.md) に従う（`~/ws` 配下の全リポジトリ共通）。

各リポジトリ固有の事情は本ファイルに追記する。

## 共通規約からの逸脱

### commit / push は自動で行う

共通規約は「commit / push は、利用者が明示的に指示したときだけ実行する」としているが、
**本リポジトリでは修正のたびに自動で `git add` / `commit` / `push` する。**

2026-09-20 に利用者から本リポジトリ限定の指示があったため。
他のリポジトリには適用しない。

コミット前には共通規約の検査を実行する。

```bash
../ws-conventions/bin/check-markdown.sh .
../ws-conventions/bin/check-privacy.sh .
../ws-conventions/bin/check-commands.sh .
python3 scripts/check_docs.py
python3 -m unittest discover -s tests -t .
```

**5 つとも指摘 0 件でなければコミットしない。** 既知の誤検知は残っていない
（`check-privacy.sh` がプレースホルダを資格情報と誤認していた件は、2026-09-20 に
ws-conventions 側で解消済み）。指摘が出たら内容を確認して直すこと。

実装（`backlog_issue_cloner.py` ・ `menu.py`）に手を入れたときは、
テストが退行を検知できるかも測る。

```bash
python3 scripts/mutation_test.py
```

**`check-terms.sh` と `gen-decision-index.py` は、本リポジトリでは使わない。**
どちらも ADR を使うリポジトリ向けで、本リポジトリは ADR を置いていない。
とくに `check-terms.sh` は `docs/term-rules.md` が無いと何もせず終了するため、
**手順に並べると実行したつもりになる。** 設計判断は `docs/DESIGN.md` に集約している。
