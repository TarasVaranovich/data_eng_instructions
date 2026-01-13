from pathlib import Path
from data_eng_instructions.constant.relative_path import PROJECT_SUB_DIR


def project_root() -> str:
    root_path: str = str(Path(__file__).resolve().parent.parent)
    return  "".join(root_path.rsplit(PROJECT_SUB_DIR , 1))