"""Archive generated baseline evidence without copying datasets or model checkpoints."""
import hashlib
import json
import platform
import shutil
import subprocess
from pathlib import Path

target = Path("docs/evidence")
target.mkdir(parents=True, exist_ok=True)
for source, relative in [(Path("results/scaling-v1"), "scaling"),
                         (Path("results/recovery.json"), "recovery.json"),
                         (Path("results/backlog.json"), "backlog.json"),
                         (Path("results/heterosplit-v1/metrics.json"), "heterosplit/metrics.json"),
                         (Path("results/heterosplit-v1/summary.md"), "heterosplit/summary.md")]:
    if not source.exists():
        raise SystemExit(f"missing evidence: {source}")
    destination = target / relative
    destination.parent.mkdir(parents=True, exist_ok=True)
    if source.is_dir():
        shutil.copytree(source, destination, dirs_exist_ok=True)
    else:
        shutil.copyfile(source, destination)
coverage = subprocess.check_output(["go", "tool", "cover", "-func=coverage.out"], text=True)
(target / "go-coverage.txt").write_text(coverage)
python_coverage = subprocess.check_output([".venv/bin/python", "-m", "coverage", "report"], text=True)
(target / "python-coverage.txt").write_text(python_coverage)
hardware = {"platform": platform.platform(), "python": platform.python_version(),
            "docker": json.loads(subprocess.check_output(["docker", "info", "--format", '{{json .}}'], text=True))}
# Keep capacity only; avoid archiving unrelated Docker labels/proxy configuration.
hardware["docker"] = {k: hardware["docker"][k] for k in ("NCPU", "MemTotal", "Architecture", "ServerVersion", "OperatingSystem")}
if platform.system() == "Darwin":
    hardware["host"] = {k: subprocess.check_output(["sysctl", "-n", k], text=True).strip()
                        for k in ("machdep.cpu.brand_string", "hw.ncpu", "hw.memsize")}
(target / "hardware.json").write_text(json.dumps(hardware, indent=2))
files = sorted(p for p in target.rglob("*") if p.is_file() and p.name != "SHA256SUMS")
(target / "SHA256SUMS").write_text("".join(hashlib.sha256(p.read_bytes()).hexdigest() + "  " + str(p.relative_to(target)) + "\n" for p in files))
print(f"Archived {len(files)} files to {target}")
