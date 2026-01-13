import data_eng_instructions.utils.local_path as local_path


def test_project_root():
    root: str = local_path.project_root()
    assert root != ""