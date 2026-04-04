from framework import metadata_driven_table

@metadata_driven_table(
    yaml_config_path='../data_sources/customers.yaml',
    archive_files=False
)
def ingest_customer_table():
    """
    Creates tables but doesn't configure file archiving.
    Useful during development or when using external archival processes.
    """
    pass