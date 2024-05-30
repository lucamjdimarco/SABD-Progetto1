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

# Caricamento del file Parquet da HDFS
csv = spark.read.parquet("hdfs://namenode:8020/nifi/filter1/raw_data_medium-utv_sorted.csv")

# Registrare il DataFrame come una tabella temporanea
csv.createOrReplaceTempView("hard_disk_failures")

# Creare una subquery per estrarre e rinominare i campi necessari
extracted_query = """
SELECT 
    date,
    vault_id.member0 AS vault_id,
    failure.member0 AS failure
FROM 
    hard_disk_failures
"""

extracted_df = spark.sql(extracted_query)
extracted_df.createOrReplaceTempView("extracted_failures")

# Eseguire la query principale su questa nuova tabella temporanea
sql_query = """
SELECT 
    date,
    vault_id,
    failure,
    COUNT(*) AS count
FROM 
    extracted_failures
GROUP BY 
    date, 
    vault_id, 
    failure
HAVING 
    failure = 1 AND COUNT(*) IN (2, 3, 4)
"""

filteredPlus = spark.sql(sql_query)

# Convertire il DataFrame risultante in una lista di dizionari JSON
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

# Scrivere i dati in Redis come hash
for item in data_to_redis:
    key = f"{item['date']}_{item['vault_id']}_{item['failure']}"
    value = item["count"]
    redis_client.hset("query1", key, value)

# Mostrare i risultati
filteredPlus.orderBy("date").show(filteredPlus.count(), truncate=False)

print("--- %s seconds ---" % (time.time() - start_time))

spark.stop()
