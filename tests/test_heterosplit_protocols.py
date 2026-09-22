"""Research dependencies are optional; grid tests also run in core CI."""
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "examples" / "heterosplit"))
from protocols import ORIGINAL, REGIMES, load_records, split_protocol
from submit import definitions


def test_versioned_grid():
    expanded = list(definitions("test"))
    original = list(definitions("test", study="original-v1"))
    assert len(expanded) == 160
    assert len(original) == 80
    assert all(len(set(regimes)) == 8 for regimes in REGIMES.values())
    assert len({j["idempotency_key"] for j in expanded + original}) == 240
    assert all("expanded-v2" in j["command"] for j in expanded)
    with pytest.raises(ValueError):
        list(definitions("test", seeds=0))


def test_month_is_utc_and_requires_timestamp(tmp_path):
    pytest.importorskip("heterosplit")
    path = tmp_path / "ratings.csv"
    path.write_text("userId,movieId,rating,timestamp\n1,2,4,1580515199\n1,3,5,1580515200\n")
    records = load_records("MovieLens", path, 10)
    assert records.codebooks["rating_month"].decode(records.codes("rating_month")).tolist() == ["2020-01", "2020-02"]
    assert "rating_month" not in load_records("MovieLens", path, 10, "original-v1").schema.roles
    path.write_text("userId,movieId,rating\n1,2,4\n")
    with pytest.raises(ValueError, match="timestamp"):
        load_records("MovieLens", path, 10)


@pytest.mark.parametrize("dataset", ["DrugComb", "MovieLens"])
def test_all_contracts_and_determinism(dataset):
    np = pytest.importorskip("numpy")
    pytest.importorskip("heterosplit")
    from heterosplit.records import PredictionRecords
    from heterosplit.schema import EntityRole, TaskSchema

    # Dense context coverage, sparse endpoints: enough records for strict intersections.
    rng = np.random.default_rng(47)
    a, b, c = rng.integers(0, 100, size=(3, 40000))
    if dataset == "DrugComb":
        roles = {"drug_row": EntityRole.source("drug"), "drug_col": EntityRole.destination("drug"),
                 "cell_line": EntityRole.context("cell_line")}
        edge = ("drug", "synergy", "drug")
        columns = dict(zip(roles, [a.astype(str), b.astype(str), c.astype(str)]))
    else:
        roles = {"user": EntityRole.source("user"), "movie": EntityRole.destination("movie"),
                 "rating_month": EntityRole.context("rating_month")}
        edge = ("user", "rates", "movie")
        columns = dict(zip(roles, [a, b, c]))
    records = PredictionRecords.from_columns(TaskSchema(edge, roles), columns)
    for regime in REGIMES[dataset]:
        result = split_protocol(records, dataset, regime, 7)
        repeated = split_protocol(records, dataset, regime, 7)
        assert result.manifest.digest() == repeated.manifest.digest()
        assert all(n > 0 for n in result.counts.values())
        assert sum(result.counts.values()) + result.n_excluded == records.n_records
        # Independently verify each explicitly held-out context, including pair keys.
        for entity, mode in (result.spec.holdout or {}).items():
            if mode != "all":
                continue
            name = next(n for n, role in result.records.schema.roles.items() if role.entity_type == entity)
            sets = [set(result.records.codes(name)[result.indices(s)]) for s in result.split_names]
            assert not (sets[0] & sets[1] or sets[0] & sets[2] or sets[1] & sets[2])
        if regime == "pair_cell_cold_start":
            swapped = columns | {"drug_row": columns["drug_col"], "drug_col": columns["drug_row"]}
            reverse = split_protocol(PredictionRecords.from_columns(TaskSchema(edge, roles), swapped), dataset, regime, 7)
            assert np.array_equal(result.record_split, reverse.record_split)
    with pytest.raises(ValueError, match="not part"):
        split_protocol(records, dataset, REGIMES[dataset][-1], 7, "original-v1")
