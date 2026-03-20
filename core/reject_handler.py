def write_rejected(df, table_name):
    if df.rdd.isEmpty():
        return

    df.write.mode("append").saveAsTable(table_name)
