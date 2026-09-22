"""Versioned research protocols; month contexts are not forecasting tasks."""
import csv
import json
from datetime import datetime, timezone
from itertools import islice

ORIGINAL = {
    "DrugComb": ["random", "pair_cold_start", "either_cold_start", "both_cold_start"],
    "MovieLens": ["random", "source_cold_start", "destination_cold_start", "both_cold_start"],
}
REGIMES = {
    "DrugComb": ORIGINAL["DrugComb"] + ["cell_cold_start", "pair_cell_cold_start",
                                          "either_cell_cold_start", "both_cell_cold_start"],
    "MovieLens": ORIGINAL["MovieLens"] + ["month_cold_start", "source_month_cold_start",
                                            "destination_month_cold_start", "both_month_cold_start"],
}
STUDIES = {"original-v1": ORIGINAL, "expanded-v2": REGIMES}


def load_records(dataset, path, max_rows, study="expanded-v2"):
    from heterosplit.datasets.drugcomb import load_drugcomb_csv
    from heterosplit.datasets.movielens import load_movielens_csv
    from heterosplit.records import PredictionRecords
    from heterosplit.schema import EntityRole, TaskSchema

    if study not in STUDIES or dataset not in STUDIES[study]:
        raise ValueError("unknown study or dataset")
    if max_rows <= 0:
        raise ValueError("max_rows must be positive")
    if dataset == "DrugComb":
        return load_drugcomb_csv(path, max_rows=max_rows, with_label=False)
    if study == "original-v1":
        return load_movielens_csv(path, max_rows=max_rows, with_label=False)
    columns = {"user": [], "movie": [], "rating_month": []}
    timestamps = []
    with open(path, newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        if not {"userId", "movieId", "rating", "timestamp"} <= set(reader.fieldnames or []):
            raise ValueError("expanded MovieLens requires userId,movieId,rating,timestamp")
        for row in islice(reader, max_rows):
            timestamp = int(row["timestamp"])
            month = datetime.fromtimestamp(timestamp, timezone.utc).strftime("%Y-%m")
            if not row["userId"] or not row["movieId"]:
                raise ValueError("missing MovieLens endpoint")
            columns["user"].append(row["userId"])
            columns["movie"].append(row["movieId"])
            columns["rating_month"].append(month)
            timestamps.append(timestamp)
    schema = TaskSchema(("user", "rates", "movie"), {
        "user": EntityRole.source("user"), "movie": EntityRole.destination("movie"),
        "rating_month": EntityRole.context("rating_month"),
    })
    return PredictionRecords.from_columns(schema, columns, timestamps=timestamps)


def split_protocol(records, dataset, regime, seed, study="expanded-v2"):
    from heterosplit import SplitSpec, split_records
    from heterosplit.records import PredictionRecords
    from heterosplit.schema import EntityRole, TaskSchema

    if regime not in STUDIES[study][dataset]:
        raise ValueError(f"{regime} is not part of {study}/{dataset}")
    holdout = None
    native_regime = regime
    if regime in ("cell_cold_start", "month_cold_start"):
        native_regime = "context_cold_start"
    elif regime not in ORIGINAL[dataset]:
        native_regime = "joint_cold_start"
        if dataset == "DrugComb":
            holdout = {"cell_line": "all"}
            if regime == "pair_cell_cold_start":
                # An unordered pair is a grouping key, never a directed drug role.
                columns = {name: records.codebooks[role.entity_type].decode(records.codes(name))
                           for name, role in records.schema.roles.items()}
                columns["drug_pair"] = [json.dumps(sorted([str(a), str(b)]))
                                        for a, b in zip(columns["drug_row"], columns["drug_col"])]
                roles = dict(records.schema.roles) | {"drug_pair": EntityRole.context("drug_pair")}
                records = PredictionRecords.from_columns(TaskSchema(records.schema.supervision_edge, roles), columns)
                holdout["drug_pair"] = "all"
            else:
                holdout["drug"] = "either" if regime == "either_cell_cold_start" else "both"
        else:
            holdout = {"rating_month": "all"}
            if regime in ("source_month_cold_start", "both_month_cold_start"):
                holdout["user"] = "source"
            if regime in ("destination_month_cold_start", "both_month_cold_start"):
                holdout["movie"] = "destination"
    spec = SplitSpec(supervision_edge=records.schema.supervision_edge, roles=dict(records.schema.roles),
                     regime=native_regime, holdout=holdout, ratios=(0.8, 0.1, 0.1), seed=seed,
                     undirected_pairs=dataset == "DrugComb")
    result = split_records(records, spec)
    result.audit.raise_for_leakage()
    if any(count == 0 for count in result.counts.values()):
        raise ValueError(f"{dataset}/{regime}/{seed}: empty partition; use more data, never relax the contract")
    return result
