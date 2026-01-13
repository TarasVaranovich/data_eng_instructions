from pathlib import Path


def project_root() -> Path:
    PROJECT_ROOT = Path(__file__).resolve().parent.parent
    return f"{PROJECT_ROOT}"