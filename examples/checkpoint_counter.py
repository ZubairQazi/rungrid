"""Kill the worker mid-run; its successor resumes the last acknowledged checkpoint."""
import json
import time
from rungrid import checkpoint, resume_path, output_dir

path = resume_path()
state = json.loads(path.read_text()) if path else {"step": 0}
print(json.dumps({"resumed_from": state["step"]}), flush=True)
for step in range(state["step"], 20):
    time.sleep(0.25)
    checkpoint({"step": step + 1})
    print(json.dumps({"step": step + 1}), flush=True)
(output_dir() / "metrics.json").write_text(json.dumps({"completed_steps": 20}))
