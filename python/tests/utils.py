import json
from pathlib import Path


def get_repo_root() -> Path:
    current_dir = Path(".").absolute()

    while current_dir.stem != "geoarrow-rs":
        if current_dir == Path("/"):
            raise ValueError("Could not find repo root; is it named geoarrow-rs?")
        current_dir = current_dir.parent

    return current_dir


REPO_ROOT = get_repo_root()
FIXTURES_DIR = REPO_ROOT / "fixtures"


def geo_interface_equals(d1, d2):
    """Compare two __geo_interface__ dictionaries for equality.

    This handles list/tuple equality
    """
    return json.loads(json.dumps(d1)) == json.loads(json.dumps(d2))
