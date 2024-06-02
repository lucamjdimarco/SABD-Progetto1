from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, percent_rank
from pyspark.sql.window import Window
import time
import redis

# Configurazione della connessione a Redis
redis_client = redis.StrictRedis(host='redis', port=6379, db=0)

spark = SparkSession.builder \
    .appName("Query3") \
    .getOrCreate()

# df = spark.read.parquet("hdfs://namenode:8020/nifi/raw_data_medium-utv_sorted.csv")

# # Filtraggio dei dischi rigidi falliti
# failures = df.filter(col("failure").getField("member0") == 1)

# # Visualizzazione dei primi 10 risultati della colonna s9_power_on_hours (member0)
# failures.select(col("s9_power_on_hours").getField("member0").alias("s9_power_on_hours")).show(10)

# spark.stop()

########################################
start_time = time.time()

# Leggi i dati
df = spark.read.parquet("hdfs://namenode:8020/nifi/filter2/raw_data_medium-utv_sorted.csv")

# Determina la data di rilevamento più recente per ciascun disco rigido
latest_detection_date = df.groupBy("serial_number") \
    .agg(expr("max(date)").alias("latest_detection_date"))

# latest_detection_date.show()



# Unisci la data di rilevamento più recente con i dati originali
#df_with_latest_date = df.join(latest_detection_date, "serial_number")
df_with_latest_date = df.join(
    latest_detection_date, 
    (df["serial_number"] == latest_detection_date["serial_number"]) &
    (df["date"] == latest_detection_date["latest_detection_date"]),
    how="inner"
)

df_with_latest_date.show(100)
# num_tuples = df_with_latest_date.count()
# print("#####################################################")
# print(f"Number of tuples: {num_tuples}")
# print("#####################################################")

# Calcola le ore di funzionamento per ciascun disco rigido
df_with_hours = df_with_latest_date.withColumn(
    "operating_hours",
    expr("s9_power_on_hours.member0")
)

# Filtra i dischi rigidi falliti e non falliti
failed_disks = df_with_hours.filter(col("failure").getField("member0") == 1)
non_failed_disks = df_with_hours.filter(col("failure").getField("member0") == 0)

# Calcola i valori minimi, massimi e percentili delle ore di funzionamento per i dischi falliti
failed_stats = failed_disks.selectExpr(
    "min(operating_hours) as min_hours",
    "percentile_approx(operating_hours, 0.25) as percentile_25",
    "percentile_approx(operating_hours, 0.5) as median_hours",
    "percentile_approx(operating_hours, 0.75) as percentile_75",
    "max(operating_hours) as max_hours",
    "count(*) as total_events"
)

# Calcola i valori minimi, massimi e percentili delle ore di funzionamento per i dischi non falliti
non_failed_stats = non_failed_disks.selectExpr(
    "min(operating_hours) as min_hours",
    "percentile_approx(operating_hours, 0.25) as percentile_25",
    "percentile_approx(operating_hours, 0.5) as median_hours",
    "percentile_approx(operating_hours, 0.75) as percentile_75",
    "max(operating_hours) as max_hours",
    "count(*) as total_events"
)

# Visualizza i risultati
print("Failed Disks Statistics:")
failed_stats.show()

print("Non-failed Disks Statistics:")
non_failed_stats.show()

failed_stats.write.mode("overwrite").csv("file:///opt/spark/work-dir/query3_1")
non_failed_stats.write.mode("overwrite").csv("file:///opt/spark/work-dir/query3_2")

# Scrittura delle statistiche su Redis
failed_stats_data = failed_stats.collect()[0].asDict()
non_failed_stats_data = non_failed_stats.collect()[0].asDict()

redis_client.hmset("failed_stats", failed_stats_data)
redis_client.hmset("non_failed_stats", non_failed_stats_data)

print("--- %s seconds ---" % (time.time() - start_time))

# Chiudi la sessione Spark
spark.stop()

