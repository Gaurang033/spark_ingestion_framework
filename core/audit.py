import uuid

from pyspark.sql.functions import current_timestamp, input_file_name, lit


def add_audit_columns(df):
    ingestion_id = str(uuid.uuid4())

    return (
        df.withColumn("ingestion_id", lit(ingestion_id))
        .withColumn("process_timestamp", current_timestamp())
        .withColumn("src_file_name", input_file_name())
    )


def create_audit_record(spark, table_name, record_count):
    return spark.createDataFrame(
        [
            {
                "table_name": table_name,
                "record_count": record_count,
            }
        ]
    )
