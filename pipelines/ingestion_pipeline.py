from decorators.yaml_table import yaml_table


@yaml_table(table="sales", config_path="config/sales_pipeline.yml")
def sales():
    pass


@yaml_table(table="customers", config_path="config/customer_pipeline.yml")
def customers():
    pass
