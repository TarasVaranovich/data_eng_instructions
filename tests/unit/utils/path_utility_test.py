from ftplib import print_line
import data_eng_instructions.utils.path_utility as local_path
from data_eng_instructions.constant.relative_path import MAINTENANCE_EVENTS

def test_project_root():
    root: str = local_path.project_root()
    # TODO: apply logging
    print_line(f"\nRoot: {root}")
    assert root != ""

def test_entity_from_path():
    entity: str = local_path.entity_from_path(MAINTENANCE_EVENTS)
    # TODO: apply logging
    print_line(f"\nRoot: {entity}")
    assert entity == "maintenance_events"