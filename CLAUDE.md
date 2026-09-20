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
python3 tools/check_docs.py
python3 -m unittest discover -s tests -t .
```

### check-privacy.sh の 2 件は誤検知として許容する

`check-privacy.sh` は次の 2 件を「資格情報らしき値」として報告し、
「コミットしないでください」と出力するが、**いずれもプレースホルダであり実際の鍵ではない。**

| 箇所 | 内容 |
| --- | --- |
| `README.md` の設定例 | `api_key` に利用者へ入力を促すプレースホルダを書いている |
| `config.sample.yaml` | 同上 |

（本節に実際の文字列を書き写すと、この検査の指摘が 2 件増えてしまうため書かない。）

実鍵を書く `config.yaml` は `.gitignore` で除外済みで、Git の追跡対象に入っていない。

**この 2 件が出ている状態でもコミットする。** ただし報告が常態化すると本物の混入を
見逃すため、`check-privacy.sh` 側でプレースホルダを除外する改修を
[../proposals/ws-conventions-privacy-placeholder.md](../proposals/ws-conventions-privacy-placeholder.md) に提案している。
これが取り込まれたら本節は削除する。

**新たな指摘が出た場合は誤検知と決めつけず、内容を確認すること。**
