#!/usr/bin/env python3
"""Generate a sample batch of prompts for load-testing the API.

Usage:
    python scripts/generate_prompts.py 1000 > data/prompts_1000.json
"""
from __future__ import annotations

import json
import sys

TOPICS = [
    "summarize the French Revolution",
    "write a haiku about distributed systems",
    "explain backpressure in async pipelines",
    "translate 'good morning' to Japanese",
    "describe the CAP theorem in one sentence",
    "list three uses for a worker pool",
]


def main() -> None:
    n = int(sys.argv[1]) if len(sys.argv) > 1 else 1000
    prompts = [
        {"id": f"prompt-{i}", "prompt": f"{TOPICS[i % len(TOPICS)]} (#{i})"}
        for i in range(n)
    ]
    json.dump({"prompts": prompts}, sys.stdout, indent=2)


if __name__ == "__main__":
    main()
