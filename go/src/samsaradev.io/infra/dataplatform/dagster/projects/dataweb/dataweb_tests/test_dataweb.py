from dataweb import defs


def test_load_definitions():
    # Load Definitions to resolve Dagster objects
    repo = defs.get_repository_def()
    repo.load_all_definitions()
    return
