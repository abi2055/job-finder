from __future__ import annotations

import json
from pathlib import Path
from typing import Any


DEFAULT_CONFIG: dict[str, Any] = {
    "sources": [
        {
            "type": "github_raw",
            "name": "simplifyjobs_summer_2026_listings",
            "url": (
                "https://raw.githubusercontent.com/"
                "SimplifyJobs/Summer2026-Internships/dev/.github/scripts/listings.json"
            ),
        },
        {
            "type": "github_raw",
            "name": "simplifyjobs_summer_2026_readme",
            "url": (
                "https://raw.githubusercontent.com/"
                "SimplifyJobs/Summer2026-Internships/dev/README.md"
            ),
        },
        {
            "type": "github_raw",
            "name": "simplifyjobs_new_grad_listings",
            "url": (
                "https://raw.githubusercontent.com/"
                "SimplifyJobs/New-Grad-Positions/dev/.github/scripts/listings.json"
            ),
        },
        {
            "type": "github_raw",
            "name": "simplifyjobs_new_grad_readme",
            "url": (
                "https://raw.githubusercontent.com/"
                "SimplifyJobs/New-Grad-Positions/dev/README.md"
            ),
        },
        {
            "type": "github_raw",
            "name": "pitt_csc_summer_2024_readme",
            "url": "https://raw.githubusercontent.com/pittcsc/Summer2024-Internships/dev/README.md",
        },
    ]
}


def load_config(path: Path | None) -> dict[str, Any]:
    if path is None:
        return DEFAULT_CONFIG

    with path.open("r", encoding="utf-8") as config_file:
        return json.load(config_file)

