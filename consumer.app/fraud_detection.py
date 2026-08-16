from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType

# Instancia a sessão com o conector Kafka 3.5.0
spark = SparkSession.builder \
    .appName("DetectorDeFraudes") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

schema = StructType([
    StructField("transaction_id", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("timestamp", LongType(), True)
])

# Leitura do Kafka
df_kafka = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "transacoes-brutas") \
    .option("startingOffsets", "latest") \
    .load()

# Transformação dos dados
df_parsed = df_kafka.selectExpr("CAST(value AS STRING) as json_payload") \
    .select(from_json(col("json_payload"), schema).alias("data")) \
    .select("data.*")

df_fraudes = df_parsed.withColumn("is_fraud", col("amount") > 10000)

# Gravação no HDFS
query = df_fraudes.writeStream \
    .format("parquet") \
    .option("path", "hdfs://namenode:9000/data-lake/transacoes/") \
    .option("checkpointLocation", "hdfs://namenode:9000/checkpoints/transacoes/") \
    .outputMode("append") \
    .start()

query.awaitTermination()