##/opt/spark/bin/spark-submit --master spark://spark-master:7077 --deploy-mode client --total-executor-cores 1 --executor-memory 1G query1.py

from pyspark.sql.functions import col
from pyspark.sql import SparkSession
import redis
import time
import json

# Configurazione della connessione a Redis
redis_client = redis.StrictRedis(host='redis', port=6379, db=0)

spark = SparkSession.builder \
  .appName("Query1") \
    .getOrCreate()

start_time = time.time()

csv = spark.read.parquet("hdfs://namenode:8020/nifi/filter1/raw_data_medium-utv_sorted.csv") 

#grouped = csv.groupBy("date","vault_id","failure").count() 
grouped = csv.groupBy(
    col("date"),
    col("vault_id").getField("member0").alias("vault_id"),
    col("failure").getField("member0").alias("failure")
).count()

filtered = grouped.filter(col("failure") == 1) 

filteredPlus = filtered.filter(col("count").isin([2,3,4]))

# Converti il DataFrame in una lista di dizionari JSON
filteredPlus.write.mode("overwrite").csv("file:///opt/spark/work-dir/query1")

print("--- %s seconds ---" % (time.time() - start_time))
print("\n\n\n")
filtered_data = filteredPlus.collect()
data_to_redis = []
for row in filtered_data:
    date = row["date"].split("T")[0]
    data_to_redis.append({
        "date": date,
        "vault_id": row["vault_id"],
        "failure": row["failure"],
        "count": row["count"]
    })

# Scrivi i dati in Redis
for item in data_to_redis:
    key = f"{item['date']}_{item['vault_id']}_{item['failure']}"
    value = item["count"]
    redis_client.hset("query1", key, value)



filteredPlus.orderBy(col("date")).show(filteredPlus.count(), truncate=False) 


print("--- %s seconds ---" % (time.time() - start_time))

print("Job completed. Keeping Spark session open for monitoring.")
while True:
     time.sleep(60)

spark.stop()