from pyspark.sql import SparkSession
import time
import redis

# Configurazione della connessione a Redis
redis_client = redis.StrictRedis(host='redis', port=6379, db=0)

# Creazione della sessione Spark
spark = SparkSession.builder \
    .appName("Query3") \
    .getOrCreate()

start_time = time.time()

# Leggi i dati
df = spark.read.parquet("hdfs://namenode:8020/nifi/filter2/raw_data_medium-utv_sorted.csv")

# Registra il DataFrame come una tabella temporanea
df.createOrReplaceTempView("hard_disk_failures")

# Query per determinare la data di rilevamento più recente per ciascun disco rigido
latest_detection_date_query = """
SELECT 
    serial_number,
    MAX(date) AS latest_detection_date
FROM 
    hard_disk_failures
GROUP BY 
    serial_number
"""
latest_detection_date = spark.sql(latest_detection_date_query)
latest_detection_date.createOrReplaceTempView("latest_detection_date")

# Query per unire la data di rilevamento più recente con i dati originali
df_with_latest_date_query = """
SELECT 
    hdf.*,
    latest_detection_date.latest_detection_date
FROM 
    hard_disk_failures hdf
JOIN 
    latest_detection_date
ON 
    hdf.serial_number = latest_detection_date.serial_number AND
    hdf.date = latest_detection_date.latest_detection_date
"""
df_with_latest_date = spark.sql(df_with_latest_date_query)
df_with_latest_date.createOrReplaceTempView("df_with_latest_date")

# Query per calcolare le ore di funzionamento per ciascun disco rigido
df_with_hours_query = """
SELECT 
    *,
    s9_power_on_hours.member0 AS operating_hours
FROM 
    df_with_latest_date
"""
df_with_hours = spark.sql(df_with_hours_query)
df_with_hours.createOrReplaceTempView("df_with_hours")

# Query per filtrare i dischi rigidi falliti e non falliti
failed_disks_query = """
SELECT 
    * 
FROM 
    df_with_hours 
WHERE 
    failure.member0 = 1
"""
failed_disks = spark.sql(failed_disks_query)
failed_disks.createOrReplaceTempView("failed_disks")

non_failed_disks_query = """
SELECT 
    * 
FROM 
    df_with_hours 
WHERE 
    failure.member0 = 0
"""
non_failed_disks = spark.sql(non_failed_disks_query)
non_failed_disks.createOrReplaceTempView("non_failed_disks")

# Query per calcolare i valori minimi, massimi e percentili delle ore di funzionamento per i dischi falliti
failed_stats_query = """
SELECT 
    MIN(operating_hours) as min_hours,
    PERCENTILE_APPROX(operating_hours, 0.25) as percentile_25,
    PERCENTILE_APPROX(operating_hours, 0.5) as median_hours,
    PERCENTILE_APPROX(operating_hours, 0.75) as percentile_75,
    MAX(operating_hours) as max_hours,
    COUNT(*) as total_events
FROM 
    failed_disks
"""
failed_stats = spark.sql(failed_stats_query)

# Query per calcolare i valori minimi, massimi e percentili delle ore di funzionamento per i dischi non falliti
non_failed_stats_query = """
SELECT 
    MIN(operating_hours) as min_hours,
    PERCENTILE_APPROX(operating_hours, 0.25) as percentile_25,
    PERCENTILE_APPROX(operating_hours, 0.5) as median_hours,
    PERCENTILE_APPROX(operating_hours, 0.75) as percentile_75,
    MAX(operating_hours) as max_hours,
    COUNT(*) as total_events
FROM 
    non_failed_disks
"""
non_failed_stats = spark.sql(non_failed_stats_query)

# Visualizza i risultati
print("Failed Disks Statistics:")
failed_stats.show()

print("Non-failed Disks Statistics:")
non_failed_stats.show()

failed_stats.write.mode("overwrite").csv("file:///opt/spark/work-dir/query3_1SQL")
non_failed_stats.write.mode("overwrite").csv("file:///opt/spark/work-dir/query3_2SQL")

Without_write_time = time.time() - start_time

# Scrittura delle statistiche su Redis
failed_stats_data = failed_stats.collect()[0].asDict()
non_failed_stats_data = non_failed_stats.collect()[0].asDict()

redis_client.hmset("failed_stats", failed_stats_data)
redis_client.hmset("non_failed_stats", non_failed_stats_data)

print("without write time: ", Without_write_time)
print("--- %s seconds ---" % (time.time() - start_time))

# Chiudi la sessione Spark
spark.stop()
