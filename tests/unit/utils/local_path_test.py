from ftplib import print_line
import data_eng_instructions.utils.local_path as local_path

def test_project_root():
    root: str = local_path.project_root()
    # TODO: apply logging
    print_line(f"\nRoot: {root}")
    assert root != ""