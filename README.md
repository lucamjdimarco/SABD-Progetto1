# SABD - Progetto 1

Di seguito vengono descritti i passi per il setup dell'ambiente.

- Eseguire ```docker-compose up -d ``` nella cartella dove presente il file docker-compose.yml
- Navigare, mediante browser, nella interfaccia di NiFi all'indirizzo ```localhost:8080/nifi```
- Caricare il template presente all'interno del repository ed avviare tutti i processori presenti. Successivamente attendere il termine del pre-processamento ed ingestion verso HDFS
- Per effettuare le query all'interno di Spark, muoversi all'interno del container Docker spark-master mediante il comando da terminale: ```docker exec -it spark-master /bin/bash```
- I comandi per effettuare le query vengono illustrati di seguito.


## Query 1

Per eseguire Query 1 con singolo core per executor 

```bash
  /opt/spark/bin/spark-submit --master spark://spark-master:7077 --deploy-mode client --total-executor-cores 1 --executor-memory 1G query1.py
```

Per eseguire Query 1 con tre core distribuiti tra gli executor 

```bash
  /opt/spark/bin/spark-submit --master spark://spark-master:7077 --deploy-mode client --total-executor-cores 3 --executor-memory 1G query1.py
```

Per eseguire Query 1 con sei core distribuiti tra gli executor 

```bash
  /opt/spark/bin/spark-submit --master spark://spark-master:7077 --deploy-mode client --total-executor-cores 6 --executor-memory 1G query1.py
```

## Query 2

Per eseguire Query 2 con singolo core per executor 

```bash
  /opt/spark/bin/spark-submit --master spark://spark-master:7077 --deploy-mode client --total-executor-cores 1 --executor-memory 1G query1.py
```

Per eseguire Query 2 con tre core distribuiti tra gli executor 

```bash
  /opt/spark/bin/spark-submit --master spark://spark-master:7077 --deploy-mode client --total-executor-cores 3 --executor-memory 1G query2.py
```

Per eseguire Query 2 con sei core distribuiti tra gli executor 

```bash
  /opt/spark/bin/spark-submit --master spark://spark-master:7077 --deploy-mode client --total-executor-cores 6 --executor-memory 1G query2.py
```

## Query 3

Per eseguire Query 3 con singolo core per executor 

```bash
  /opt/spark/bin/spark-submit --master spark://spark-master:7077 --deploy-mode client --total-executor-cores 1 --executor-memory 1G query3.py
```

Per eseguire Query 3 con tre core distribuiti tra gli executor 

```bash
  /opt/spark/bin/spark-submit --master spark://spark-master:7077 --deploy-mode client --total-executor-cores 3 --executor-memory 1G query3.py
```

Per eseguire Query 3 con sei core distribuiti tra gli executor 

```bash
  /opt/spark/bin/spark-submit --master spark://spark-master:7077 --deploy-mode client --total-executor-cores 6 --executor-memory 1G query3.py
```


## Query 1 SQL

Per eseguire Query 1 con singolo core per executor 

```bash
  /opt/spark/bin/spark-submit --master spark://spark-master:7077 --deploy-mode client --total-executor-cores 1 --executor-memory 1G query1SQL.py
```

Per eseguire Query 1 con tre core distribuiti tra gli executor 

```bash
  /opt/spark/bin/spark-submit --master spark://spark-master:7077 --deploy-mode client --total-executor-cores 3 --executor-memory 1G query1SQL.py
```

Per eseguire Query 1 con sei core distribuiti tra gli executor 

```bash
  /opt/spark/bin/spark-submit --master spark://spark-master:7077 --deploy-mode client --total-executor-cores 6 --executor-memory 1G query1SQL.py
```

## Query 2

Per eseguire Query 2 con singolo core per executor 

```bash
  /opt/spark/bin/spark-submit --master spark://spark-master:7077 --deploy-mode client --total-executor-cores 1 --executor-memory 1G query2SQL.py
```

Per eseguire Query 2 con tre core distribuiti tra gli executor 

```bash
  /opt/spark/bin/spark-submit --master spark://spark-master:7077 --deploy-mode client --total-executor-cores 3 --executor-memory 1G query2SQL.py
```

Per eseguire Query 2 con sei core distribuiti tra gli executor 

```bash
  /opt/spark/bin/spark-submit --master spark://spark-master:7077 --deploy-mode client --total-executor-cores 6 --executor-memory 1G query2SQL.py
```

## Query 3

Per eseguire Query 3 con singolo core per executor 

```bash
  /opt/spark/bin/spark-submit --master spark://spark-master:7077 --deploy-mode client --total-executor-cores 1 --executor-memory 1G query3SQL.py
```

Per eseguire Query 3 con tre core distribuiti tra gli executor 

```bash
  /opt/spark/bin/spark-submit --master spark://spark-master:7077 --deploy-mode client --total-executor-cores 3 --executor-memory 1G query3SQL.py
```

Per eseguire Query 3 con sei core distribuiti tra gli executor 

```bash
  /opt/spark/bin/spark-submit --master spark://spark-master:7077 --deploy-mode client --total-executor-cores 6 --executor-memory 1G query3SQL.py
```
