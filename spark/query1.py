from pyspark.sql.functions import col
from pyspark.sql import SparkSession
import redis
import time

# Configurazione della connessione a Redis
redis_client = redis.StrictRedis(host='redis', port=6379, db=0)

spark = SparkSession.builder \
  .appName("Query1") \
    .getOrCreate()

start_time = time.time()

csv = spark.read.parquet("hdfs://namenode:8020/nifi/raw_data_medium-utv_sorted.csv") 

csv.printSchema()


#grouped = csv.groupBy("date","vault_id","failure").count() 
grouped = csv.groupBy(
    col("date"),
    col("vault_id").getField("member0").alias("vault_id"),
    col("failure").getField("member0").alias("failure")
).count()

filtered = grouped.filter(col("failure") == 1) 

filteredPlus = filtered.filter(col("count").isin([2,3,4])) 

# Salvataggio dei dati su Redis
filteredPlus_data = filteredPlus.collect()
for row in filteredPlus_data:
    key = f"{row['date']}_{row['vault_id']}_{row['failure']}"
    value = row["count"]
    redis_client.set(key, value)


filteredPlus.orderBy(col("date")).show(filteredPlus.count(), truncate=False) 

print("--- %s seconds ---" % (time.time() - start_time))

spark.stop()