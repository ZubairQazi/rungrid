# Research provenance

`research.py` contains model and evaluation helpers from Zubair Qazi's MIT-licensed
[HeteroSplit](https://github.com/ZubairQazi/heterosplit),
`experiments/regime_study.py` at revision `9636559fc724d8b162afcfcadfa2ea1c385ed292`.
Copyright 2026 Zubair Qazi. RunGrid's MIT license preserves those terms.
`task.py` adapts that implementation with independent jobs and checkpointed model,
optimizer, RNG state, configuration identity, and split-manifest validation.

The existing study has **eight dataset/regime combinations**, four per dataset.
Two models and five seeds therefore yield **80 jobs**, not 160. The original spec's
eight regimes *per dataset* cannot be applied unchanged: unordered DrugComb pairs
reject source/destination regimes, and the MovieLens schema has no context role.
This demo preserves the research semantics. Adding new schema/regime combinations
requires a separate research decision; no dummy duplicate regimes are counted.

DrugComb and MovieLens data are not redistributed. Mount your legally obtained data
directory read-only. The default uses 10,000 rows and three epochs for a scheduler
demonstration, not an accuracy reproduction. Use the original study's data/epoch
settings when reproducing its research numbers. Data license terms remain applicable.
