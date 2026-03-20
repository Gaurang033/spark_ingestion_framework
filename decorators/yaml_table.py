from functools import wraps

from pyspark import pipelines as dp

from core.audit import add_audit_columns, create_audit_record
from core.reader import read_source
from core.reject_handler import write_rejected
from core.transformer import apply_mappings
from core.validator import split_valid_invalid
from utils.yaml_loader import load_config


def yaml_table(table: str, config_path: str):
    def decorator(func):

        # ✅ Load config per decorator
        config = load_config(config_path)

        if table not in config["tables"]:
            raise ValueError(f"Table '{table}' not found in {config_path}")

        table_cfg = config["tables"][table]

        dp_args = {
            "name": table_cfg["target"]["name"],
            "partition_cols": table_cfg["target"].get("partition_cols"),
        }

        dp_args = {k: v for k, v in dp_args.items() if v is not None}

        @wraps(func)
        def wrapper():
            spark = func.__globals__["spark"]

            # 1. Read
            df = read_source(spark, table_cfg["source"])

            # 2. Mapping
            df = apply_mappings(df, table_cfg.get("schema"))

            # 3. Validation
            valid_df, invalid_df = split_valid_invalid(df, table_cfg.get("schema"))

            # 4. Audit columns
            valid_df = add_audit_columns(valid_df)

            # 5. Reject handling
            if "reject" in table_cfg:
                write_rejected(invalid_df, table_cfg["reject"]["table"])

            # 6. Audit table
            if table_cfg.get("audit", {}).get("enabled"):
                count = valid_df.count()

                audit_df = create_audit_record(spark, dp_args["name"], count)

                audit_df.write.mode("append").saveAsTable(f"{dp_args['name']}_audit")

            return valid_df

        return dp.table(**dp_args)(wrapper)

    return decorator
