# Expanded study v2

Two datasets, eight contracts each, MF and SAGE, seeds 0–4: **160 job definitions**.
This is an exploratory scheduler/research extension, not a reproduction of the
original study. `--study original-v1` retains its four regimes per dataset.

| DrugComb protocol | Held-out condition |
|---|---|
| random | Random observations; no entity-disjoint guarantee |
| pair_cold_start | Unordered drug pair |
| either_cold_start | At least one drug endpoint unseen relative to training |
| both_cold_start | Both drug endpoints held out |
| cell_cold_start | Cell line |
| pair_cell_cold_start | Unordered pair AND cell line |
| either_cell_cold_start | At least one drug endpoint AND cell line |
| both_cell_cold_start | Both drug endpoints AND cell line |

| MovieLens protocol | Held-out condition |
|---|---|
| random | Random observations; no entity-disjoint guarantee |
| source_cold_start | User |
| destination_cold_start | Movie |
| both_cold_start | User AND movie |
| month_cold_start | UTC rating calendar month |
| source_month_cold_start | User AND month |
| destination_month_cold_start | Movie AND month |
| both_month_cold_start | User AND movie AND month |

## Schema and assignment

DrugComb uses the existing cell-line field. Pair–cell splitting adds a context
grouping key made from a JSON-encoded sorted pair of drug IDs, so swapping drug
columns cannot change the pair identity. It is not a fabricated source/destination
orientation. Both grouping keys must be disjoint across retained partitions.

MovieLens v2 requires `timestamp` and derives `rating_month` as UTC `YYYY-MM`.
It retains the raw timestamps in the record fingerprint. These calendar groups
are randomly assigned by the seeded HeteroSplit group allocator, **not ordered
chronologically**. Training may contain later dates than test; this evaluates
held-out observation contexts, not future recommendation. No extra download or
user demographics are invented. Missing/malformed timestamps fail explicitly.

Ratios remain 0.8/0.1/0.1. Joint contracts independently assign each active axis
and retain a record only when all axes agree on its partition. Cross-partition
records are excluded, never moved to weaken the contract. Achieved counts and
exclusions are emitted by preflight and training metrics; empty partitions fail.
Strict intersections may leave very small evaluation sets, so report their sizes
and do not interpret five seeds as independent population uncertainty.

## Modeling limitations

MF/SAGE remain **context-blind link predictors**, predicting observed endpoint
links against sampled nonlinks, not synergy labels or numeric ratings. Context
controls the split, not model input. Context-only protocols may share endpoint
pairs across partitions: pair-disjointness is guaranteed only where specified.
The train message-passing graph still excludes held-out supervision pairs under
HeteroSplit's policy. Negative sampling excludes known positives across the full
dataset, as in the original study; this is not a deployment-time candidate model.
Do not describe these results as context-aware synergy prediction or forecasting.

Study ID, regime, and manifest digest enter checkpoint identity; job idempotency
keys/artifact prefixes include study ID. Aggregation groups by study to avoid
mixing historical and expanded results. Use a new run ID if epochs/data/settings
change. The historical 80-job evidence is preserved; a 160-job definition alone
does not establish 160 successful executions.

## Validation

Run preflight before submission using the same data and maximum-row setting:

```sh
python examples/heterosplit/preflight.py --data-dir /path/to/data --study expanded-v2 --output results/expanded-preflight.json
python examples/heterosplit/submit.py --run-id expanded-1 --study expanded-v2
```

Preflight covers 80 dataset/regime/seed combinations, shared by both models.
Core tests check the 160/80 job counts and version separation. With the research
dependencies installed, `pytest tests/test_heterosplit_protocols.py` also checks
UTC month boundaries, required columns, split determinism, contracts, and pair
orientation invariance. Optional dependency skips are not full research validation.
The dedicated research-contracts CI job installs the pinned HeteroSplit source
so these contract tests do not skip there.

For a real-data, one-epoch check of both models on all 16 combinations, including
reload of each completed checkpoint and identical evaluation metrics:

```sh
python examples/heterosplit/smoke.py --data-dir /path/to/data --output results/expanded-smoke-1
```

Use a fresh output directory. This runs 32 local model/checkpoint round trips,
not distributed worker recovery or the full five-seed grid.
