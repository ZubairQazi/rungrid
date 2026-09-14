"""Create reproducible Go release archives for Linux/macOS amd64/arm64."""
import argparse
import gzip
import hashlib
import io
import os
import subprocess
import tarfile
from pathlib import Path

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("--check", action="store_true")
args = parser.parse_args()
if subprocess.check_output(["git", "status", "--porcelain"], text=True).strip():
    raise SystemExit("release requires a clean worktree")
if args.check:
    raise SystemExit(0)
revision = subprocess.check_output(["git", "rev-parse", "--short", "HEAD"], text=True).strip()
tag = subprocess.run(["git", "describe", "--tags", "--exact-match"], text=True, capture_output=True)
version = tag.stdout.strip() if tag.returncode == 0 else f"dev-{revision}"
destination = Path("dist")
destination.mkdir(exist_ok=True)
checksums = []
for system in ("linux", "darwin"):
    for architecture in ("amd64", "arm64"):
        env = os.environ | {"GOOS": system, "GOARCH": architecture, "CGO_ENABLED": "0"}
        binaries = []
        for name, source in (("server", "./cmd/server"), ("rungrid", "./cmd/cli")):
            target = destination / f"{name}-{system}-{architecture}"
            subprocess.run([os.environ.get("GO", "go"), "build", "-trimpath", "-ldflags=-s -w", "-o", str(target), source], env=env, check=True)
            binaries.append((name, target.read_bytes()))
        filename = destination / f"rungrid-{version}-{system}-{architecture}.tar.gz"
        with filename.open("wb") as raw, gzip.GzipFile(fileobj=raw, mode="wb", mtime=0, filename="") as compressed:
            with tarfile.open(fileobj=compressed, mode="w") as archive:
                for name, data in binaries + [("LICENSE", Path("LICENSE").read_bytes()), ("README.md", Path("README.md").read_bytes())]:
                    info = tarfile.TarInfo(name)
                    info.size = len(data)
                    info.mode = 0o755 if name in ("server", "rungrid") else 0o644
                    archive.addfile(info, io.BytesIO(data))
        checksums.append(hashlib.sha256(filename.read_bytes()).hexdigest() + "  " + filename.name)
(destination / "SHA256SUMS").write_text("\n".join(checksums) + "\n")
print(f"Built {version}: {len(checksums)} archives in {destination}")
