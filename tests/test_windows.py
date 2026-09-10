"""
Windows 環境への対応を守るテスト
================================
macOS / Linux では気づけない退行を検出する。

Windows の日本語コンソールは既定で CP932 のため、CP932 に無い文字を
print すると文字化けではなく UnicodeEncodeError で落ちる。
menu.bat をダブルクリックした場合、メニューの最初の画面すら出ない。
"""

import pathlib
import re
import unittest

ROOT = pathlib.Path(__file__).resolve().parent.parent

# 出力に使うソース（cmd.exe のコンソールに出るもの）
OUTPUT_SOURCES = ("backlog_issue_cloner.py", "menu.py")


class TestCp932Safety(unittest.TestCase):
    """画面に出る文字がすべて CP932 で表現できること。"""

    def _unencodable(self, text: str) -> set:
        bad = set()
        for ch in set(text):
            if ord(ch) < 128:
                continue
            try:
                ch.encode("cp932")
            except UnicodeEncodeError:
                bad.add(ch)
        return bad

    def test_sources_are_cp932_safe(self):
        for name in OUTPUT_SOURCES:
            with self.subTest(file=name):
                bad = self._unencodable((ROOT / name).read_text(encoding="utf-8"))
                self.assertEqual(
                    bad, set(),
                    f"{name} に CP932 で表現できない文字があります: "
                    + ", ".join(f"U+{ord(c):04X} {c!r}" for c in sorted(bad))
                    + "。Windows の日本語コンソールで UnicodeEncodeError になります。",
                )

    def test_detects_a_known_bad_character(self):
        """検査自体が機能していることを確かめる（em dash は CP932 に無い）。"""
        self.assertEqual(self._unencodable("作成完了 — 件名"), {"—"})

    def test_common_symbols_are_safe(self):
        """実際に使っている記号が CP932 にあること。"""
        for ch in "←※【】―":
            with self.subTest(char=ch):
                self.assertEqual(self._unencodable(ch), set())


class TestMenuBat(unittest.TestCase):
    """menu.bat が cmd.exe で正しく解釈される形であること。"""

    def setUp(self):
        self.path = ROOT / "menu.bat"
        self.raw = self.path.read_bytes()

    def test_exists(self):
        self.assertTrue(self.path.is_file())

    def test_line_endings_are_crlf(self):
        """LF だけだと cmd.exe が誤動作する。Git の正規化にも注意。"""
        lone_lf = self.raw.count(b"\n") - self.raw.count(b"\r\n")
        self.assertEqual(lone_lf, 0, "CRLF でない行があります（.gitattributes を確認）")

    def test_is_ascii_only(self):
        """コンソールの文字コードに依存しないよう ASCII に限る。"""
        non_ascii = [b for b in self.raw if b > 127]
        self.assertEqual(non_ascii, [], "menu.bat に非 ASCII 文字が含まれています")

    def test_uses_pushd_not_cd(self):
        """cd はネットワーク共有（UNC パス）で失敗する。"""
        text = self.raw.decode("ascii")
        self.assertIn('pushd "%~dp0"', text)
        self.assertNotIn("cd /d", text)

    def test_pauses_before_closing(self):
        """ダブルクリック起動でウィンドウが即閉じるとエラーが読めない。"""
        self.assertIn("pause", self.raw.decode("ascii"))

    def test_falls_back_when_py_launcher_is_missing(self):
        text = self.raw.decode("ascii")
        self.assertIn("py -3", text)
        self.assertIn("python --version", text)

    def test_runs_the_menu(self):
        self.assertIn("menu.py", self.raw.decode("ascii"))


class TestGitAttributes(unittest.TestCase):
    """Git が menu.bat の改行コードを LF に正規化しないこと。"""

    def test_bat_is_pinned_to_crlf(self):
        path = ROOT / ".gitattributes"
        self.assertTrue(path.is_file(), ".gitattributes がありません")
        text = path.read_text(encoding="utf-8")
        self.assertTrue(
            re.search(r"^\*\.bat\s+.*eol=crlf", text, re.M),
            "*.bat に eol=crlf の指定がありません",
        )


if __name__ == "__main__":
    unittest.main(verbosity=2)
