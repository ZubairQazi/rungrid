"""Execute an importable trusted Python function as a scheduled command."""
import argparse
import importlib
import json
from .checkpoint import output_dir


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("function", help="installed.module:function")
    parser.add_argument("--args", default="[]")
    parser.add_argument("--kwargs", default="{}")
    args = parser.parse_args()
    module, name = args.function.split(":", 1)
    function = getattr(importlib.import_module(module), name)
    result = function(*json.loads(args.args), **json.loads(args.kwargs))
    (output_dir() / "result.json").write_text(json.dumps(result, allow_nan=False))


if __name__ == "__main__":
    main()
