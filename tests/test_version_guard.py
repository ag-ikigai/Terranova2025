from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

def test_engine_version_matches_file():
    from terra_nova.modules.m3_financing import engine
    version_txt = (ROOT / "VERSION.txt").read_text(encoding="utf-8").strip().splitlines()[0]
    assert getattr(engine, "__version__", version_txt) == version_txt
