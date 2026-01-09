
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, when
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import firebase_admin
from firebase_admin import credentials, firestore




KAFKA_BOOTSTRAP_SERVERS = "vitalstream-kafka-stephen-c769.d.aivencloud.com:16892"



# Kafka SSL options

KAFKA_SSL_OPTIONS = {
    "kafka.bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
    "kafka.security.protocol": "SSL",
    "kafka.ssl.ca.location": "kafka-certs/ca.pem",
    "kafka.ssl.certificate.location": "kafka-certs/service.cert",
    "kafka.ssl.key.location": "kafka-certs/service.key",
}



# Spark Session

spark = SparkSession.builder \
    .appName("VitalStreamProcessor") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")



# VITALS STREAM (continuous)

vitals_raw = spark.readStream \
    .format("kafka") \
    .options(**KAFKA_SSL_OPTIONS) \
    .option("subscribe", "patient_vitals") \
    .option("startingOffsets", "latest") \
    .load()

vitals_schema = StructType([
    StructField("patient_id", StringType()),
    StructField("heart_rate", IntegerType()),
    StructField("spo2", IntegerType()),
    StructField("timestamp", StringType())
])

vitals = (
    vitals_raw
    .selectExpr("CAST(value AS STRING) AS json")
    .select(from_json(col("json"), vitals_schema).alias("data"))
    .select("data.*")
)



# THRESHOLDS (treated as reference data)

thresholds_raw = spark.readStream \
    .format("kafka") \
    .options(**KAFKA_SSL_OPTIONS) \
    .option("subscribe", "patient_thresholds") \
    .option("startingOffsets", "latest") \
    .load()

threshold_schema = StructType([
    StructField("patient_id", StringType()),
    StructField("max_heart_rate", IntegerType()),
    StructField("min_spo2", IntegerType())
])

thresholds_stream = (
    thresholds_raw
    .selectExpr("CAST(value AS STRING) AS json")
    .select(from_json(col("json"), threshold_schema).alias("data"))
    .select("data.*")
)

# Convert thresholds stream → memory table
thresholds_query = (
    thresholds_stream
    .writeStream
    .format("memory")
    .queryName("thresholds_table")
    .outputMode("append")
    .start()
)



# JOIN vitals stream WITH in-memory thresholds table

thresholds_static = spark.sql("SELECT * FROM thresholds_table")

joined = vitals.join(
    thresholds_static,
    on="patient_id",
    how="left"
)



# ALERT LOGIC

processed = joined.withColumn(
    "status",
    when(
        (col("max_heart_rate").isNotNull()) &
        (
            (col("heart_rate") > col("max_heart_rate")) |
            (col("spo2") < col("min_spo2"))
        ),
        "CRITICAL"
    ).otherwise("NORMAL")
)



# FIRESTORE INIT

cred = credentials.Certificate("firebase-key.json")
firebase_admin.initialize_app(cred)
db = firestore.client()



# WRITE TO FIRESTORE

def write_to_firestore(batch_df, batch_id):
    for row in batch_df.collect():
        db.collection("alerts").document(row.patient_id).set({
            "patient_id": row.patient_id,
            "heart_rate": row.heart_rate,
            "spo2": row.spo2,
            "status": row.status,
            "timestamp": row.timestamp
        })


query = (
    processed.writeStream
    .foreachBatch(write_to_firestore)
    .outputMode("update")
    .option("checkpointLocation", "./checkpoints/vitalstream")
    .start()
)

query.awaitTermination()
