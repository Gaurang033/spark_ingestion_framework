def read_source(spark, source_config):
    fmt = source_config["format"]

    if fmt in ["csv", "json", "parquet"]:
        reader = spark.read.format(fmt)

        for k, v in source_config.get("options", {}).items():
            reader = reader.option(k, v)

        return reader.load(source_config["path"])

    if fmt == "jdbc":
        return (
            spark.read.format("jdbc")
            .options(
                url=source_config["url"],
                dbtable=source_config["table"],
                **source_config.get("properties", {}),
            )
            .load()
        )

    raise ValueError(f"Unsupported format: {fmt}")
