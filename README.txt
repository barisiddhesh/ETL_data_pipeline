This is a quick demo of how a data pipeline looks like in real world.

Tech stack used: Python, Pyspark, HDFS, Airflow, Linux and Shell script.

Work flow: Airflow or Cron --> shell script (run.sh) --> shell script (env.sh) --> Pyspark script --> HDFS storage --> shell script (run.sh) --> Output

How to execute using Cron:
1. In Linux terminal, type crontab -e
2. Later, paste * * * * * /mnt/c/users/'siddhesh bari'/dp/spark-apps/S260100/shell/run.sh "prod"
3. As per the cron expression, this pipeline runs every minute.

** Via Airflow, pipeline will run everyday at 3pm.
