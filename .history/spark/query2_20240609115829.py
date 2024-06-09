#/opt/spark/bin/spark-submit --master spark://spark-master:7077 --deploy-mode client --num-executors 2 --executor-cores 1 --executor-memory 1G query2.py

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, desc, collect_set, concat_ws
import time
import redis

redis_client = redis.StrictRedis(host='redis', port=6379, db=0)

spark = SparkSession.builder \
    .appName("Query2") \
    .getOrCreate()

start_time = time.time()

df = spark.read.parquet("hdfs://namenode:8020/nifi/filter1/raw_data_medium-utv_sorted.csv")

# Filtro per fallimenti (failure == 1)
failures = df.filter(col("failure").getField("member0") == 1)

# Raggruppamento per modello e conteggio dei guasti
model_failures = failures.groupBy("model").count()

# Ordinamento dei modelli per numero di guasti in ordine decrescente
sorted_failures = model_failures.orderBy(desc("count"))

# Selezione dei primi 10 modelli con più guasti
top_10_failures = sorted_failures.limit(10)

top_10_failures.write.mode("overwrite").csv("file:///opt/spark/work-dir/query2_1")

print("---First part: %s seconds ---\n\n\n" % (time.time() - start_time))
first_part_time = time.time() - start_time
# Visualizzazione dei risultati
top_10_failures.show(truncate=False)

# Scrittura dei top 10 modelli su Redis
top_10_vault_failures_data = top_10_failures.collect()
data_to_redis = []
for row in top_10_vault_failures_data:
    data_to_redis.append({
        "model": row["model"],
        "count": row["count"]
    })

for item in data_to_redis:
    key = f"{item['model']}"
    value = item["count"]
    redis_client.hset("query2a", key, value)

print("---End first part: %s seconds ---\n\n\n" % (time.time() - start_time))
end_first_part_time = time.time() - start_time


# Raggruppamento per vault e conteggio dei guasti
vault_failures = failures.groupBy(col("vault_id").getField("member0").alias("vault_id")).count()

# Ordinamento dei vault per numero di guasti in ordine decrescente
sorted_vault_failures = vault_failures.orderBy(desc("count"))

# Selezione dei primi 10 vault con più guasti
top_10_vault_failures = sorted_vault_failures.limit(10)

# Visualizzazione dei risultati
top_10_vault_failures.show(truncate=False)

df_csv = failures.groupBy(col("vault_id").getField("member0").alias("vault_id")) \
    .agg(concat_ws(",", collect_set("model")).alias("models"))

# Ottenere la lista dei modelli di hard disk per ciascun vault
vaults_with_models = failures.groupBy(col("vault_id").getField("member0").alias("vault_id")) \
    .agg(collect_set("model").alias("models"))

# Unione con il conteggio dei guasti per ottenere il numero di guasti per ciascun vault
result = top_10_vault_failures.join(vaults_with_models, "vault_id").orderBy(desc("count"))

result_csv = top_10_vault_failures.join(df_csv, "vault_id").orderBy(desc("count"))

result_csv.write.mode("overwrite").csv("file:///opt/spark/work-dir/query2_2")

print("---Second part: %s seconds ---" % (time.time() - start_time))
second_part_time = time.time() - start_time

# Visualizzazione del risultato finale
result.show(truncate=False)

# Itera su ciascuna riga del DataFrame result e scrivi i dati in Redis
for row in result.collect():
    vault_id = row["vault_id"]
    count = row["count"]
    models = row["models"]
    
    # Componi la chiave Redis utilizzando vault_id e models
    redis_key = f"{vault_id}_{'_'.join(models)}"
    
    # Scrivi i dati in Redis
    redis_client.hset("query2b",redis_key, count)



# result_data = result.collect()
# for row in result_data:
#     vault_id = row["vault_id"]
#     models = ",".join(row["models"])
#     count = row["count"]
#     redis_key = f"vault_{vault_id}, models:{models}"
#     redis_value = f"count:{count}"
#     redis_client.set(redis_key, redis_value)

print("--- %s seconds ---" % (time.time() - start_time))
print("First part time: %s" % first_part_time)  
print("End first part time: %s" % end_first_part_time)
print("Second part time: %s" % second_part_time)

print("Job completed. Keeping Spark session open for monitoring.")
while True:
    time.sleep(60)

spark.stop()
