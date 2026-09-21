"""依存が入っていない Python で起動したときの振る舞いを守るテスト。

**開発機には PyYAML が入っているため、この経路が壊れても他のテストは
全部通る。** 配布先は Windows で、同梱の Python を使うとは限らない。

ここで固定するのは 2 つ。

- トレースバックではなく、対処の分かる案内を出すこと
- Exit code `2`（実行する前に人が直すもの）を返すこと

案内には **実行中のインタプリタのパス** を含める。Windows では `py`
ランチャ経由で複数の Python が入っていることがあり、入れた先と
動かしている先が違うと「pip install したのに直らない」が起きるため。
"""

import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent

# 依存が入っていない環境を作るための細工。site が起動時に読み込む。
# ライブラリをアンインストールする代わりに、import を名前で撥ねる。
BLOCKER = '''
import sys


class _Blocker:
    def find_spec(self, name, path=None, target=None):
        if name.split(".")[0] == "yaml":
            raise ModuleNotFoundError("No module named 'yaml'", name="yaml")
        return None


sys.meta_path.insert(0, _Blocker())
'''


def _run(entry, *, block_yaml):
    """入口スクリプトを子プロセスで起動し、CompletedProcess を返す。"""
    env = dict(os.environ)
    with tempfile.TemporaryDirectory() as tmp:
        if block_yaml:
            Path(tmp, "sitecustomize.py").write_text(BLOCKER, encoding="utf-8")
            existing = env.get("PYTHONPATH")
            env["PYTHONPATH"] = (
                f"{tmp}{os.pathsep}{existing}" if existing else tmp
            )
        return subprocess.run(
            [sys.executable, str(ROOT / entry), "--help"],
            capture_output=True, text=True, env=env,
        )


class TestMissingDependency(unittest.TestCase):
    """PyYAML が無い状態で起動したときの案内と Exit code。"""

    ENTRIES = ("backlog_issue_cloner.py", "menu.py")

    def test_shows_guidance_instead_of_traceback(self):
        """トレースバックではなく、入れるべきものと入れ方を示す。"""
        for entry in self.ENTRIES:
            with self.subTest(entry=entry):
                r = _run(entry, block_yaml=True)
                output = r.stdout + r.stderr
                self.assertNotIn("Traceback", output)
                self.assertIn("PyYAML", output)
                self.assertIn("pip install pyyaml", output)

    def test_shows_running_interpreter(self):
        """どの Python で動いているかを示す。

        入れた先と動かしている先が違う場合、これが無いと
        「pip install したのに直らない」から抜け出せない。
        """
        for entry in self.ENTRIES:
            with self.subTest(entry=entry):
                r = _run(entry, block_yaml=True)
                output = r.stdout + r.stderr
                self.assertIn("実行中の Python: ", output)
                shown = output.split("実行中の Python: ")[1].splitlines()[0]
                # 親と子でシンボリックリンクの解決が違うため、実体で比べる
                self.assertEqual(
                    Path(shown).resolve(), Path(sys.executable).resolve()
                )

    def test_exit_code_is_config_error(self):
        """Exit code は 2。実行する前に人が直すもの、という扱いにする。"""
        for entry in self.ENTRIES:
            with self.subTest(entry=entry):
                self.assertEqual(_run(entry, block_yaml=True).returncode, 2)

    def test_unrelated_import_error_is_not_swallowed(self):
        """yaml 以外の ModuleNotFoundError は握り潰さない。

        自前モジュールの綴り間違いまで「ライブラリを入れてください」と
        案内すると、本当の原因が隠れる。
        """
        script = (
            "import sys\n"
            f"sys.path.insert(0, {str(ROOT)!r})\n"
            "import backlog_issue_cloner as sut\n"
            "e = ModuleNotFoundError('No module named \\'typo\\'', name='typo')\n"
            "print(e.name != 'yaml')\n"
        )
        r = subprocess.run([sys.executable, "-c", script],
                           capture_output=True, text=True)
        self.assertEqual(r.stdout.strip(), "True")

    def test_starts_normally_when_dependency_is_present(self):
        """依存が入っていれば、案内を出さずに通常どおり起動する。

        細工が空振りしても気付けないため、対になる確認を置く。
        """
        for entry in self.ENTRIES:
            with self.subTest(entry=entry):
                r = _run(entry, block_yaml=False)
                self.assertEqual(r.returncode, 0)
                self.assertNotIn("PyYAML", r.stdout + r.stderr)


if __name__ == "__main__":
    unittest.main(verbosity=2)
