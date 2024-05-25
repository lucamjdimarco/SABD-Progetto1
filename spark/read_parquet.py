from pyspark.sql import SparkSession

# Crea una sessione Spark
spark = SparkSession.builder \
    .appName("Read Parquet from HDFS") \
    .master("spark://spark-master:7077") \
    .getOrCreate()

# Leggi i dati Parquet da HDFS
df = spark.read.parquet("hdfs://namenode:8020/nifi/raw_data_medium-utv_sorted.csv")

# Mostra i dati
df.show()

# Esegui qualche operazione sui dati
df.printSchema()
df.select("date", "serial_number", "model", "failure", "vault_id", "s9_power_on_hours").show()

# Scrivi i dati in un nuovo file Parquet su HDFS
df.write.parquet("hdfs://namenode:8020/nifi/processed_data.parquet")
