from pyspark.sql import SparkSession
import time
import redis

# Configurazione della connessione a Redis
redis_client = redis.StrictRedis(host='redis', port=6379, db=0)

# Creazione della sessione Spark
spark = SparkSession.builder \
    .appName("Query2") \
    .getOrCreate()

start_time = time.time()

# Caricamento del file Parquet da HDFS
df = spark.read.parquet("hdfs://namenode:8020/nifi/filter1/raw_data_medium-utv_sorted.csv")

# Registrare il DataFrame come una tabella temporanea
df.createOrReplaceTempView("hard_disk_failures")

# Query per filtrare i fallimenti
failures_query = """
SELECT 
    *,
    failure.member0 as failure_value,
    vault_id.member0 as vault_id_value
FROM 
    hard_disk_failures
WHERE 
    failure.member0 = 1
"""
failures_df = spark.sql(failures_query)
failures_df.createOrReplaceTempView("failures")

# Query per raggruppamento per modello e conteggio dei guasti
model_failures_query = """
SELECT 
    model,
    COUNT(*) as count
FROM 
    failures
GROUP BY 
    model
ORDER BY 
    count DESC
LIMIT 10
"""
top_10_failures = spark.sql(model_failures_query)

top_10_failures.write.mode("overwrite").csv("file:///opt/spark/work-dir/query2_1SQL")
print("---First part: %s seconds ---" % (time.time() - start_time))
first_part_time = time.time() - start_time

# Visualizzazione dei risultati
top_10_failures.show(truncate=False)

# Scrittura dei top 10 modelli su Redis
top_10_failures_data = top_10_failures.collect()
data_to_redis = []
for row in top_10_failures_data:
    data_to_redis.append({
        "model": row["model"],
        "count": row["count"]
    })

for item in data_to_redis:
    key = f"{item['model']}"
    value = item["count"]
    redis_client.hset("query2a", key, value)

end_first_part_time = time.time() - start_time

# Query per raggruppamento per vault e conteggio dei guasti
vault_failures_query = """
SELECT 
    vault_id_value as vault_id,
    COUNT(*) as count
FROM 
    failures
GROUP BY 
    vault_id_value
ORDER BY 
    count DESC
LIMIT 10
"""
top_10_vault_failures = spark.sql(vault_failures_query)

# Visualizzazione dei risultati
top_10_vault_failures.show(truncate=False)

# Query per ottenere la lista dei modelli di hard disk per ciascun vault
vaults_with_models_query = """
SELECT 
    vault_id_value as vault_id,
    COLLECT_SET(model) as models
FROM 
    failures
GROUP BY 
    vault_id_value
"""
vaults_with_models = spark.sql(vaults_with_models_query)
vaults_with_models.createOrReplaceTempView("vaults_with_models")

# Unione con il conteggio dei guasti per ottenere il numero di guasti per ciascun vault
result_query = """
SELECT 
    v.vault_id,
    v.count,
    m.models
FROM 
    (SELECT 
        vault_id_value as vault_id,
        COUNT(*) as count
    FROM 
        failures
    GROUP BY 
        vault_id_value
    ORDER BY 
        count DESC
    LIMIT 10) v
JOIN 
    vaults_with_models m
ON 
    v.vault_id = m.vault_id
ORDER BY 
    v.count DESC
"""
result = spark.sql(result_query)

# result.write.mode("overwrite").csv("file:///opt/spark/work-dir/query2_2SQL")
print("---Second part: %s seconds ---" % (time.time() - start_time))

second_part_time = time.time() - start_time

# Visualizzazione del risultato finale
result.show(truncate=False)

# Scrittura dei risultati su Redis
for row in result.collect():
    vault_id = row["vault_id"]
    count = row["count"]
    models = row["models"]
    
    # Componi la chiave Redis utilizzando vault_id e models
    redis_key = f"{vault_id}_{'_'.join(models)}"
    
    # Scrivi i dati in Redis
    redis_client.hset("query2b", redis_key, count)

print("First part time: ", first_part_time)
print("End first part time: ", end_first_part_time)
print("Second part time: ", second_part_time)
print("--- %s seconds ---" % (time.time() - start_time))

# Chiudere la sessione Spark
spark.stop()
