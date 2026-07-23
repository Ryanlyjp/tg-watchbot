import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


class RuntimePathTest(unittest.TestCase):
    def test_data_directory_can_be_moved_outside_code_directory(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            env = os.environ.copy()
            env["TG_WATCHBOT_DATA_DIR"] = tmp
            result = subprocess.run(
                [
                    sys.executable,
                    "-c",
                    "import app; print(app.DATA_DIR); print(app.DB_PATH); print(app.ENV_PATH)",
                ],
                cwd=Path(__file__).resolve().parents[1],
                env=env,
                check=True,
                capture_output=True,
                text=True,
            )

        lines = result.stdout.splitlines()
        self.assertEqual(str(Path(tmp).resolve()), lines[0])
        self.assertEqual(str(Path(tmp).resolve() / "tg-watchbot.sqlite3"), lines[1])
        self.assertEqual(str(Path(tmp).resolve() / ".env"), lines[2])


if __name__ == "__main__":
    unittest.main()
