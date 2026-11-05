"""One scheduling decision, run from GitHub Actions (no dependency on Render being awake).
Uses the same logic and the same MongoDB state as the web service."""
import json, sys, os
os.environ.setdefault("ENABLE_SCHEDULER", "0")
import main

out = main.tick_all()
print(json.dumps(out, default=str))
sys.exit(1 if out.get("action") == "error" else 0)   # a red run = GitHub emails you
