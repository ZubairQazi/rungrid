"""Validate all dataset/regime/seed splits before starting an expensive grid."""
import argparse
from pathlib import Path
from heterosplit import SplitSpec, split_records
from heterosplit.datasets.drugcomb import load_drugcomb_csv
from heterosplit.datasets.movielens import load_movielens_csv
from submit import REGIMES

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("--data-dir", type=Path, default=Path("/data"))
parser.add_argument("--max-rows", type=int, default=120000)
args = parser.parse_args()
for dataset, regimes in REGIMES.items():
    records = (load_drugcomb_csv(args.data_dir / "summary_v_1_5.csv", max_rows=args.max_rows, with_label=False)
               if dataset == "DrugComb" else load_movielens_csv(args.data_dir / "ml/ml-latest-small/ratings.csv", max_rows=args.max_rows, with_label=False))
    if records.n_records == 0:
        raise ValueError(f"{dataset}: no usable records; increase --max-rows")
    for regime in regimes:
        for seed in range(5):
            result = split_records(records, SplitSpec(supervision_edge=records.schema.supervision_edge,
                                  roles=dict(records.schema.roles), regime=regime, ratios=(0.8, 0.1, 0.1),
                                  seed=seed, undirected_pairs=dataset == "DrugComb"))
            result.audit.raise_for_leakage()
            if result.supervision_edge_index("train").shape[1] == 0 or result.supervision_edge_index("test").shape[1] == 0:
                raise ValueError(f"{dataset}/{regime}/{seed}: empty split")
    print(f"{dataset}: {records.n_records} records; 20 regime/seed splits validated", flush=True)
