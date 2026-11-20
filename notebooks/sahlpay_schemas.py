from pyspark.sql.types import (StructType, StructField, StringType,
                               DoubleType, TimestampType)

tx_schema = StructType([
    StructField("tx_id",        StringType(),    True),
    StructField("type",         StringType(),    True),
    StructField("payer_wallet", StringType(),    True),
    StructField("payee_id",     StringType(),    True),
    StructField("device_id",    StringType(),    True),
    StructField("city",         StringType(),    True),
    StructField("channel",      StringType(),    True),
    StructField("status",       StringType(),    True),
    StructField("amount_mad",   DoubleType(),    True),
    StructField("event_time",   TimestampType(), True),
])

merchants_schema = StructType([
    StructField("merchant_id", StringType(), True),
    StructField("name",        StringType(), True),
    StructField("city",        StringType(), True),
    StructField("category",    StringType(), True),
    StructField("latitude",    DoubleType(), True),
    StructField("longitude",   DoubleType(), True),
])
