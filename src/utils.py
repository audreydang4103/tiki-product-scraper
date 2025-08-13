import json
from pathlib import Path

def load_config(path: Path):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def load_checkpoint(path: Path):
    if path.exists():
        with open(path, "r", encoding="utf-8") as f:
            return set(json.load(f))
    return set()

def save_checkpoint(path: Path, done_ids: set):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(list(done_ids), f, ensure_ascii=False, indent=2)
