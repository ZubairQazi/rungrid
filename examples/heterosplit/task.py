"""One real-data HeteroSplit experiment with checkpointed training."""
import argparse
import base64
import io
import json
import os
from pathlib import Path

import torch
import torch.nn.functional as F
from heterosplit import SplitSpec, split_records
from heterosplit.datasets.drugcomb import load_drugcomb_csv
from heterosplit.datasets.movielens import load_movielens_csv
from rungrid import checkpoint, output_dir, resume_path
from research import Encoder, _prepare, _sample_pairs, _decode, _roc_auc, _average_precision


def run(args):
    torch.set_num_threads(1)
    torch.manual_seed(args.seed)
    records = (load_drugcomb_csv(args.data, max_rows=args.max_rows, with_label=False)
               if args.dataset == "DrugComb" else load_movielens_csv(args.data, max_rows=args.max_rows, with_label=False))
    spec = SplitSpec(supervision_edge=records.schema.supervision_edge, roles=dict(records.schema.roles),
                     regime=args.regime, ratios=(0.8, 0.1, 0.1), seed=args.seed,
                     undirected_pairs=args.dataset == "DrugComb")
    result = split_records(records, spec)
    result.audit.raise_for_leakage()
    result.manifest.save(output_dir() / "split-manifest.json")
    prep = _prepare(result)
    model = Encoder(prep["num_nodes"], args.dim, use_graph=args.model == "SAGE")
    optimizer = torch.optim.Adam(model.parameters(), lr=0.01)
    generator = torch.Generator().manual_seed(args.seed)
    src = torch.arange(prep["n_src"])
    dst = src if prep["self_rel"] else torch.arange(prep["offset"], prep["num_nodes"])
    identity = {"dataset": args.dataset, "regime": args.regime, "model": args.model, "seed": args.seed,
                "epochs": args.epochs, "dim": args.dim, "manifest_digest": result.manifest.digest()}
    start = 0
    if restored := resume_path():
        state = json.loads(restored.read_text())
        if state["identity"] != identity:
            raise ValueError("checkpoint configuration mismatch")
        data = torch.load(io.BytesIO(base64.b64decode(state["torch"])), weights_only=True)
        model.load_state_dict(data["model"])
        optimizer.load_state_dict(data["optimizer"])
        generator.set_state(data["generator"])
        torch.set_rng_state(data["rng"])
        start = state["epoch"]
    print(json.dumps({"resumed_epoch": start, **identity}), flush=True)
    train, test = prep["train_pos"], prep["test_pos"]
    if train.size(1) == 0 or test.size(1) == 0:
        raise ValueError("empty split; increase --max-rows")
    for epoch in range(start, args.epochs):
        model.train()
        optimizer.zero_grad()
        z = model(prep["mp"])
        neg = _sample_pairs(src, dst, train.size(1), prep["positives"], prep["self_rel"], generator)
        scores = torch.cat([_decode(z, train), _decode(z, neg)])
        labels = torch.cat([torch.ones(train.size(1)), torch.zeros(neg.size(1))])
        loss = F.binary_cross_entropy_with_logits(scores, labels)
        loss.backward()
        optimizer.step()
        buffer = io.BytesIO()
        torch.save({"model": model.state_dict(), "optimizer": optimizer.state_dict(),
                    "generator": generator.get_state(), "rng": torch.get_rng_state()}, buffer)
        checkpoint({"identity": identity, "epoch": epoch + 1, "torch": base64.b64encode(buffer.getvalue()).decode()})
        print(json.dumps({"epoch": epoch + 1, "loss": float(loss.detach())}), flush=True)
    model.eval()
    with torch.no_grad():
        z = model(prep["mp"])
        test_src, test_dst = torch.unique(test[0]), torch.unique(test[1])
        if prep["self_rel"]:
            test_src = test_dst = torch.unique(test.reshape(-1))
        neg = _sample_pairs(test_src, test_dst, test.size(1), prep["positives"], prep["self_rel"], generator)
        scores = torch.cat([_decode(z, test), _decode(z, neg)])
        labels = torch.cat([torch.ones(test.size(1)), torch.zeros(neg.size(1))])
        metrics = identity | {"auc": _roc_auc(scores, labels), "ap": _average_precision(scores, labels),
                              "records": records.n_records, "resumed_epoch": start,
                              "job_id": os.environ["RUNGRID_JOB_ID"], "attempt_id": os.environ["RUNGRID_ATTEMPT_ID"]}
    (output_dir() / "metrics.json").write_text(json.dumps(metrics, indent=2, allow_nan=False))
    print(json.dumps(metrics, allow_nan=False), flush=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dataset", choices=["DrugComb", "MovieLens"], required=True)
    parser.add_argument("--data", type=Path, required=True)
    parser.add_argument("--regime", required=True)
    parser.add_argument("--model", choices=["MF", "SAGE"], required=True)
    parser.add_argument("--seed", type=int, required=True)
    parser.add_argument("--epochs", type=int, default=3)
    parser.add_argument("--dim", type=int, default=16)
    parser.add_argument("--max-rows", type=int, default=10000)
    run(parser.parse_args())
