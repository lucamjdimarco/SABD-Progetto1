from pyspark import SparkContext
from pyspark.sql import SparkSession

sc = SparkContext("local", "Query 1")
spark = SparkSession.builder.appName("Query 1").getOrCreate()

# Read data from HDFS
df = spark.read.parquet("hdfs://namenode:8020/nifi/raw_data_medium-utv_sorted.csv")

rdd = df.rdd

grouped_rdd = rdd.groupBy(lambda x: (x[0], x[4]))

result = grouped_rdd.mapValues(list).collect()

for key, value in result:
    print(f"Date: {key[0]}, Vault ID: {key[1]}")
    for v in value:
        print(v)


sc.stop()


########

counts = rdd.map(lambda x: ((x['date'], x['vault_id']), x['failure'])) \
            .reduceByKey(lambda a, b, c: a + b)

# 2. Filtri per il conteggio esatto
vaults_with_4_failures = counts.filter(lambda x: x[2] == 4).collect()
vaults_with_3_failures = counts.filter(lambda x: x[1] == 3).collect()
vaults_with_2_failures = counts.filter(lambda x: x[1] == 2).collect()

# Stampare i risultati
print("Vaults with exactly 4 failures:", vaults_with_4_failures)
print("Vaults with exactly 3 failures:", vaults_with_3_failures)
print("Vaults with exactly 2 failures:", vaults_with_2_failures)
