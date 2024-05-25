from pyspark.sql import SparkSession
from pyspark.sql.functions import col, desc, collect_set
import time
import redis

redis_client = redis.StrictRedis(host='redis', port=6379, db=0)

spark = SparkSession.builder \
    .appName("Query2") \
    .getOrCreate()

start_time = time.time()

df = spark.read.parquet("hdfs://namenode:8020/nifi/raw_data_medium-utv_sorted.csv")

# Filtro per fallimenti (failure == 1)
failures = df.filter(col("failure").getField("member0") == 1)

# Raggruppamento per modello e conteggio dei guasti
model_failures = failures.groupBy("model").count()

# Ordinamento dei modelli per numero di guasti in ordine decrescente
sorted_failures = model_failures.orderBy(desc("count"))

# Selezione dei primi 10 modelli con più guasti
top_10_failures = sorted_failures.limit(10)

# Visualizzazione dei risultati
top_10_failures.show(truncate=False)

# Scrittura dei top 10 modelli su Redis
# top_10_failures_data = top_10_failures.collect()
# for idx, row in enumerate(top_10_failures_data):
#     model = row["model"]
#     count = row["count"]
#     redis_key = f"top_10_failures:{idx+1}"
#     redis_value = f"model:{model},count:{count}"
#     redis_client.set(redis_key, redis_value)

# Raggruppamento per vault e conteggio dei guasti
vault_failures = failures.groupBy(col("vault_id").getField("member0").alias("vault_id")).count()

# Ordinamento dei vault per numero di guasti in ordine decrescente
sorted_vault_failures = vault_failures.orderBy(desc("count"))

# Selezione dei primi 10 vault con più guasti
top_10_vault_failures = sorted_vault_failures.limit(10)

# Scrittura dei top 10 vault su Redis
top_10_vault_failures_data = top_10_vault_failures.collect()
data_to_redis = []
for row in top_10_vault_failures_data:
    data_to_redis.append({
        "vault_id": row["vault_id"],
        "count": row["count"]
    })

for item in data_to_redis:
    key = f"{item['vault_id']}"
    value = item["count"]
    redis_client.hset("query2a", key, value)

# Visualizzazione dei risultati
top_10_vault_failures.show(truncate=False)

# Ottenere la lista dei modelli di hard disk per ciascun vault
vaults_with_models = failures.groupBy(col("vault_id").getField("member0").alias("vault_id")) \
    .agg(collect_set("model").alias("models"))

# Unione con il conteggio dei guasti per ottenere il numero di guasti per ciascun vault
result = top_10_vault_failures.join(vaults_with_models, "vault_id").orderBy(desc("count"))

# Visualizzazione del risultato finale
result.show(truncate=False)

# Scrittura dei risultati su Redis
result_data = result.collect()
for row in result_data:
    vault_id = row["vault_id"]
    models = ",".join(row["models"])
    count = row["count"]
    redis_key = f"vault_{vault_id}"
    redis_value = f"count:{count},models:{models}"
    redis_client.set(redis_key, redis_value)

print("--- %s seconds ---" % (time.time() - start_time))

spark.stop()
