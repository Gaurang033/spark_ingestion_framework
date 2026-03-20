from pyspark.sql.functions import col, expr


def apply_mappings(df, schema_config):
    if not schema_config:
        return df

    columns = schema_config.get("columns", [])
    transformed_cols = []

    for c in columns:
        src = c.get("source")
        tgt = c.get("target")
        dtype = c.get("type")

        if "transform" in c:
            col_expr = expr(c["transform"])
        else:
            col_expr = col(src)

        if dtype:
            col_expr = col_expr.cast(dtype)

        transformed_cols.append(col_expr.alias(tgt))

    return df.select(*transformed_cols)
