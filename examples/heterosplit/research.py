"""Comparative study: does the split regime change difficulty *and model rankings*?

For each dataset x regime x model x seed we train a link predictor and score held-out
edges against **type-correct, same-pool** negatives, then aggregate mean +/- std over
seeds. Two models are compared:

- **MF**   — learnable node embeddings + dot product (no graph message passing).
- **SAGE** — the same embeddings passed through a 2-layer GraphSAGE encoder over the
  training message-passing graph, then dot product.

The research question: under a *random* split the graph model (SAGE) can exploit
structure and usually beats MF; under a *cold-start* split the held-out entities are
absent from the training graph, so SAGE's advantage should shrink or vanish — i.e. the
split regime can **reorder model rankings**, not just lower scores.

Requires the ``[pyg]`` extra. Real datasets:

    # DrugComb (self-relation drug--drug): downloaded earlier to data/summary_v_1_5.csv
    # MovieLens (bipartite user--movie): auto-downloaded to data/ml/

    uv run --extra pyg python experiments/regime_study.py --seeds 5 --epochs 40 \
        --out experiments/results/regime_study.md
"""

from __future__ import annotations

import argparse
import json
import os
import statistics
from pathlib import Path
from typing import Any

import numpy as np
import torch
import torch.nn.functional as F
from torch import Tensor, nn
from torch_geometric.nn import SAGEConv

from heterosplit import SplitSpec, split_records
from heterosplit.datasets.drugcomb import load_drugcomb_csv
from heterosplit.datasets.movielens import download_movielens, load_movielens_csv
from heterosplit.records import PredictionRecords
from heterosplit.result import SplitResult

DATA_DIR = Path(__file__).resolve().parent.parent / "data"

# (regime, holdout, undirected_pairs) per dataset.
DRUGCOMB_REGIMES = [
    ("random", None, True),
    ("pair_cold_start", None, True),
    ("either_cold_start", None, True),
    ("both_cold_start", None, True),
]
MOVIELENS_REGIMES = [
    ("random", None, False),
    ("source_cold_start", None, False),  # cold user
    ("destination_cold_start", None, False),  # cold movie
    ("both_cold_start", None, False),
]


# --------------------------------------------------------------------------- model


class Encoder(nn.Module):
    def __init__(self, num_nodes: int, dim: int, use_graph: bool) -> None:
        super().__init__()
        self.embedding = nn.Embedding(num_nodes, dim)
        self.use_graph = use_graph
        if use_graph:
            self.conv1 = SAGEConv(dim, dim)
            self.conv2 = SAGEConv(dim, dim)

    def forward(self, edge_index: Tensor) -> Tensor:
        x = self.embedding.weight
        if self.use_graph:
            x = F.relu(self.conv1(x, edge_index))
            x = self.conv2(x, edge_index)
        return x


def _decode(z: Tensor, edge_index: Tensor) -> Tensor:
    return (z[edge_index[0]] * z[edge_index[1]]).sum(dim=-1)


# ------------------------------------------------------------------------- metrics


def _roc_auc(scores: Tensor, labels: Tensor) -> float:
    pos, neg = scores[labels == 1], scores[labels == 0]
    if pos.numel() == 0 or neg.numel() == 0:
        return float("nan")
    ranks = torch.cat([pos, neg]).argsort().argsort().float() + 1.0
    n_pos = float(pos.numel())
    return float((ranks[: pos.numel()].sum() - n_pos * (n_pos + 1) / 2) / (n_pos * neg.numel()))


def _average_precision(scores: Tensor, labels: Tensor) -> float:
    order = scores.argsort(descending=True)
    lab = labels[order].float()
    if lab.sum() == 0:
        return float("nan")
    cum_tp = torch.cumsum(lab, dim=0)
    precision = cum_tp / torch.arange(1, lab.numel() + 1, dtype=torch.float)
    return float((precision * lab).sum() / lab.sum())


# ------------------------------------------------------------- combined node space


def _prepare(result: SplitResult) -> dict[str, Any]:
    """Map the split into a single combined node space (bipartite -> offset dst ids)."""
    records = result.records
    schema = records.schema
    self_rel = schema.is_self_relation
    n_src = records.n_entities(schema.source_type)
    n_dst = n_src if self_rel else records.n_entities(schema.destination_type)
    offset = 0 if self_rel else n_src

    def combine(edge_index: np.ndarray) -> np.ndarray:
        return np.stack([edge_index[0], edge_index[1] + offset])

    mp_dir = combine(result.message_passing_edge_index(add_reverse=False))
    mp = np.concatenate([mp_dir, mp_dir[::-1]], axis=1)  # symmetric for message passing
    return {
        "num_nodes": n_src if self_rel else n_src + n_dst,
        "n_src": n_src,
        "offset": offset,
        "self_rel": self_rel,
        "mp": torch.as_tensor(mp, dtype=torch.long),
        "train_pos": torch.as_tensor(
            combine(result.supervision_edge_index("train")), dtype=torch.long
        ),
        "test_pos": torch.as_tensor(
            combine(result.supervision_edge_index("test")), dtype=torch.long
        ),
        "positives": _positive_set(
            combine(np.stack([records.source_codes, records.destination_codes])), self_rel
        ),
    }


def _positive_set(all_pos: np.ndarray, self_rel: bool) -> set[tuple[int, int]]:
    a, b = all_pos[0].tolist(), all_pos[1].tolist()
    if self_rel:
        return {(min(x, y), max(x, y)) for x, y in zip(a, b, strict=True)}
    return set(zip(a, b, strict=True))


def _sample_pairs(
    src_pool: Tensor,
    dst_pool: Tensor,
    k: int,
    positives: set[tuple[int, int]],
    self_rel: bool,
    generator: torch.Generator,
) -> Tensor:
    """Sample ``k`` type-correct negative pairs (src from src_pool, dst from dst_pool)."""
    if k == 0 or src_pool.numel() == 0 or dst_pool.numel() == 0:
        return torch.empty((2, 0), dtype=torch.long)
    out_s: list[int] = []
    out_d: list[int] = []
    for _ in range(40):
        a = src_pool[torch.randint(src_pool.numel(), (4 * k,), generator=generator)].tolist()
        b = dst_pool[torch.randint(dst_pool.numel(), (4 * k,), generator=generator)].tolist()
        for x, y in zip(a, b, strict=True):
            if self_rel and x == y:
                continue
            key = (min(x, y), max(x, y)) if self_rel else (x, y)
            if key in positives:
                continue
            out_s.append(x)
            out_d.append(y)
            if len(out_s) >= k:
                break
        if len(out_s) >= k:
            break
    return torch.tensor([out_s[:k], out_d[:k]], dtype=torch.long)


# ------------------------------------------------------------------------ one run


def run_once(
    records: PredictionRecords,
    regime: str,
    holdout: Any,
    undirected: bool,
    model: str,
    *,
    dim: int,
    epochs: int,
    seed: int,
    lr: float = 0.01,
) -> dict[str, float]:
    torch.manual_seed(seed)
    spec = SplitSpec(
        supervision_edge=records.schema.supervision_edge,
        roles=dict(records.schema.roles),
        regime=regime,
        ratios=(0.8, 0.1, 0.1),
        holdout=holdout,
        undirected_pairs=undirected,
        seed=seed,
    )
    prep = _prepare(split_records(records, spec))
    num_nodes, n_src, offset, self_rel = (
        prep["num_nodes"],
        prep["n_src"],
        prep["offset"],
        prep["self_rel"],
    )
    mp, train_pos, test_pos, positives = (
        prep["mp"],
        prep["train_pos"],
        prep["test_pos"],
        prep["positives"],
    )

    src_all = torch.arange(0, n_src)
    dst_all = torch.arange(0, n_src) if self_rel else torch.arange(offset, num_nodes)

    encoder = Encoder(num_nodes, dim, use_graph=(model == "SAGE"))
    optimizer = torch.optim.Adam(encoder.parameters(), lr=lr)
    gen = torch.Generator().manual_seed(seed)

    for _ in range(epochs):
        encoder.train()
        optimizer.zero_grad()
        z = encoder(mp)
        neg = _sample_pairs(src_all, dst_all, train_pos.size(1), positives, self_rel, gen)
        scores = torch.cat([_decode(z, train_pos), _decode(z, neg)])
        labels = torch.cat([torch.ones(train_pos.size(1)), torch.zeros(neg.size(1))])
        loss = F.binary_cross_entropy_with_logits(scores, labels)
        loss.backward()
        optimizer.step()

    # Fair test negatives: same node pool (and type) as the test positives.
    test_src_pool = torch.unique(test_pos[0])
    test_dst_pool = torch.unique(test_pos[1])
    if self_rel:
        pool = torch.unique(test_pos.reshape(-1))
        test_src_pool = test_dst_pool = pool
    encoder.eval()
    with torch.no_grad():
        z = encoder(mp)
        neg = _sample_pairs(
            test_src_pool, test_dst_pool, test_pos.size(1), positives, self_rel, gen
        )
        scores = torch.cat([_decode(z, test_pos), _decode(z, neg)])
        labels = torch.cat([torch.ones(test_pos.size(1)), torch.zeros(neg.size(1))])
    return {"auc": _roc_auc(scores, labels), "ap": _average_precision(scores, labels)}
