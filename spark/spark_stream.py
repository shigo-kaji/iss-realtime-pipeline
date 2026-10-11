from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, lit, current_timestamp, to_timestamp, round as sround
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

spark = (
    SparkSession.builder
    .appName("ISS_RealTime_Pipeline")
    .config("spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,"
            "com.datastax.spark:spark-cassandra-connector_2.12:3.5.1")
    .config("spark.cassandra.connection.host", "localhost")
    .config("spark.cassandra.connection.port", "9042")
    .config("spark.sql.shuffle.partitions", "2")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# Schemas that match each producer's JSON
position_schema = StructType([
    StructField("record_time", StringType()),
    StructField("latitude", DoubleType()),
    StructField("longitude", DoubleType()),
    StructField("altitude_km", DoubleType()),
    StructField("velocity_kmh", DoubleType()),
    StructField("visibility", StringType()),
    StructField("country_code", StringType()),
    StructField("location", StringType()),
])
telemetry_schema = StructType([
    StructField("record_time", StringType()),
    StructField("footprint_km", DoubleType()),
    StructField("daynum", DoubleType()),
    StructField("solar_lat", DoubleType()),
    StructField("solar_lon", DoubleType()),
    StructField("units", StringType()),
])

def read_topic(topic, schema):
    raw = (spark.readStream.format("kafka")
           .option("kafka.bootstrap.servers", "localhost:9092")
           .option("subscribe", topic)
           .option("startingOffsets", "latest")
           .load())
    return (raw.selectExpr("CAST(value AS STRING) AS json")
            .select(from_json(col("json"), schema).alias("d"))
            .select("d.*")
            .withColumn("record_time", to_timestamp(col("record_time")))
            .withColumn("satellite", lit("ISS"))
            .withColumn("processed_at", current_timestamp())
            .filter(col("record_time").isNotNull()))

# Process the fields
position_df = (read_topic("iss_position", position_schema)
               .withColumn("altitude_km", sround(col("altitude_km"), 2))
               .withColumn("velocity_kmh", sround(col("velocity_kmh"), 2)))
telemetry_df = (read_topic("iss_telemetry", telemetry_schema)
                .withColumn("footprint_km", sround(col("footprint_km"), 2)))

def write_batch(table):
    def _write(batch_df, batch_id):
        n = batch_df.count()
        print(f"[Spark] Batch {batch_id} -> {table}: {n} rows")
        if n > 0:
            batch_df.show(truncate=False)
            (batch_df.write.format("org.apache.spark.sql.cassandra")
             .options(keyspace="iss_data", table=table)
             .mode("append").save())
    return _write

q1 = (position_df.writeStream.foreachBatch(write_batch("iss_position"))
      .option("checkpointLocation", "checkpoints/iss_position")
      .trigger(processingTime="10 seconds").start())
q2 = (telemetry_df.writeStream.foreachBatch(write_batch("iss_telemetry"))
      .option("checkpointLocation", "checkpoints/iss_telemetry")
      .trigger(processingTime="10 seconds").start())

print("[Spark] Streaming started: iss_position, iss_telemetry")
spark.streams.awaitAnyTermination()
