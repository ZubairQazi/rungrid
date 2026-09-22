"""Validate all dataset/regime/seed splits before starting an expensive grid."""
import argparse
import json
from pathlib import Path
from protocols import STUDIES, load_records, split_protocol

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("--data-dir", type=Path, default=Path("/data"))
parser.add_argument("--max-rows", type=int, default=120000)
parser.add_argument("--study", choices=list(STUDIES), default="expanded-v2")
parser.add_argument("--output", type=Path)
args = parser.parse_args()
rows = []
for dataset, regimes in STUDIES[args.study].items():
    path = args.data_dir / ("summary_v_1_5.csv" if dataset == "DrugComb" else "ml/ml-latest-small/ratings.csv")
    records = load_records(dataset, path, args.max_rows, args.study)
    if records.n_records == 0:
        raise ValueError(f"{dataset}: no usable records; increase --max-rows")
    for regime in regimes:
        for seed in range(5):
            result = split_protocol(records, dataset, regime, seed, args.study)
            row = {"study": args.study, "dataset": dataset, "regime": regime, "seed": seed,
                   "records": records.n_records, "counts": result.counts,
                   "excluded": result.n_excluded, "manifest_digest": result.manifest.digest()}
            rows.append(row)
            print(json.dumps(row), flush=True)
    print(f"{dataset}: {len(regimes) * 5} regime/seed splits validated", flush=True)
if args.output:
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(rows, indent=2))
