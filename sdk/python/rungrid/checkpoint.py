"""Atomically publish checkpoints; workers discover completed files only."""
import json
import os
import uuid
from pathlib import Path


def output_dir():
    path = Path(os.environ["RUNGRID_OUTPUT_DIR"])
    path.mkdir(parents=True, exist_ok=True)
    return path


def resume_path():
    value = os.environ.get("RUNGRID_CHECKPOINT_PATH")
    return Path(value) if value else None


def checkpoint(state):
    directory = Path(os.environ["RUNGRID_CHECKPOINT_DIR"])
    directory.mkdir(parents=True, exist_ok=True)
    target = directory / f"{uuid.uuid4()}.json"
    temporary = target.with_suffix(".tmp")
    with temporary.open("w") as stream:
        json.dump(state, stream)
        stream.flush()
        os.fsync(stream.fileno())
    temporary.replace(target)
    return target
