from pyspark.sql.functions import col


def split_valid_invalid(df, schema_config):
    if not schema_config:
        return df, df.sparkSession.createDataFrame([], df.schema)

    condition = None

    for c in schema_config.get("columns", []):
        tgt = c["target"]
        nullable = c.get("nullable", True)

        if not nullable:
            rule = col(tgt).isNotNull()
            condition = rule if condition is None else condition & rule

    if condition is None:
        return df, df.sparkSession.createDataFrame([], df.schema)

    valid_df = df.filter(condition)
    invalid_df = df.filter(~condition)

    return valid_df, invalid_df
