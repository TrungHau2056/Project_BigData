from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType

ECOMMERCE_SCHEMA = StructType([
    StructField("event_time", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("product_id", IntegerType(), True),
    StructField("category_id", StringType(), True),
    StructField("category_code", StringType(), True),
    StructField("brand", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("user_id", IntegerType(), True),
    StructField("user_session", StringType(), True),
])

# NOT DONE
EVENT_SCHEMA = StructType([
   StructField("event_time", TimestampType(), True),
   StructField("event_type", StringType(), True),
   StructField("product_id", IntegerType(), True),
   StructField("category_id", StringType(), True),
   StructField("category_code", StringType(), True),
   StructField("brand", StringType(), True),
   StructField("price", DoubleType(), True),
   StructField("user_id", IntegerType(), True),
   StructField("user_session", StringType(), True),
   StructField("category_level_1", StringType(), True),
   StructField("category_level_2", StringType(), True),
   StructField("category_level_3", StringType(), True),

])