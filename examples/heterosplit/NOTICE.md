# Research provenance

`research.py` contains model and evaluation helpers from Zubair Qazi's MIT-licensed
[HeteroSplit](https://github.com/ZubairQazi/heterosplit),
`experiments/regime_study.py` at revision `9636559fc724d8b162afcfcadfa2ea1c385ed292`.
Copyright 2026 Zubair Qazi. RunGrid's MIT license preserves those terms.
`task.py` adapts that implementation with independent jobs and checkpointed model,
optimizer, RNG state, configuration identity, and split-manifest validation.

The reference study has **eight dataset/regime combinations**, four per dataset,
and remains selectable as `original-v1` (80 jobs). With user-authorized schema and
protocol expansion, `expanded-v2` supplies eight meaningful contracts per dataset
(160 jobs). These are RunGrid research extensions, not claims about the original
HeteroSplit publication. Unordered DrugComb pairs remain unordered; MovieLens
context is derived from observed UTC rating months. See [REGIMES.md](REGIMES.md).
Historical v0.1 results remain unchanged and describe only the original study.

DrugComb and MovieLens data are not redistributed. Mount your legally obtained data
directory read-only. The default reads up to 120,000 raw rows and uses three epochs for a scheduler
demonstration, not an accuracy reproduction. Use the original study's data/epoch
settings when reproducing its research numbers. Data license terms remain applicable.
The first 10,000 DrugComb rows in the reference summary are monotherapy records that
the adapter filters out; a small prefix is not a valid smoke-test dataset.
