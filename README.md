# SABD - Progetto 1

Di seguito vengono descritti i passi per il setup dell'ambiente.

- Eseguire ```docker-compose up -d ``` nella cartella dove presente il file docker-compose.yml
- Navigare, mediante browser, nella interfaccia di NiFi all'indirizzo ```localhost:8080/nifi```
- Caricare il template presente all'interno del repository ed avviare tutti i processori presenti. Successivamente attendere il termine del pre-processamento ed ingestion verso HDFS
- Muoversi nella cartella utils ed eseguire lo script ```./load.sh```
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
  /opt/spark/bin/spark-submit --master spark://spark-master:7077 --deploy-mode client --total-executor-cores 1 --executor-memory 1G query2.py
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

Per eseguire Query 1 SQL con singolo core per executor 

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

## Query 2 SQL

Per eseguire Query 2 SQL con singolo core per executor 

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

## Query 3 SQL

Per eseguire Query 3 SQL con singolo core per executor 

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

## Grafana
- Accedere all'indirizzo ```localhost:5000```
- Inserire ```username = admin``` e ```password = admin```
- Sul menù di sx, aprire la pagina ```connections```, muoversi su ```new connections``` e creare una nuova connessione verso Redis
- Successivamente, dal menù di sx, muoversi nella voce ```connections```. Successivamente selezionare la voce ```data source```: nella finestrà che si aprirà, specificare la porta sulla quale Redis sarà in ascolto: ```6379```
- Dopo aver completato con successo la connessione verso Redis, cliccare sulla voce ```new``` --> ```import``` e inserire il JSON della dashboard presente nel repository
