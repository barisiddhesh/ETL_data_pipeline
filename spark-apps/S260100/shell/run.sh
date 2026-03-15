#!/bin/bash

# Step 1: Validate Input
if [ -z "$1" ]; then
  echo "Mention dev or prod"
  exit 1
fi

ENV="$1"

if [ -f /.dockerenv ]; then
  echo " Detected: Running INSIDE container (Airflow or Jupyter)"

  #export PATH=$PATH:/opt/hadoop/bin
  source /opt/spark-apps/S260100/shell/env.sh "$ENV"

  echo "Uploading to HDFS"
  hdfs dfs -rm -r -f ${HDFS_INPUT}  
  hdfs dfs -mkdir -p ${HDFS_INPUT}
  hdfs dfs -put "${COLLECTION_PATH}" ${HDFS_INPUT}
  
  echo "Running Spark job"
  spark-submit --master spark://spark-master:7077 /opt/spark-apps/S260100/scripts/etl_job.py "$RUN_DATE" "$ENV" "$HDFS_INPUT" "$HDFS_OUTPUT"

  echo "Exporting merged CSV to local path"
  #One way (preferred)
  hdfs dfs -getmerge ${HDFS_OUTPUT}/part* /tmp/DATA_${RUN_DATE}.csv
  mkdir $FINAL_CSV
  cp /tmp/DATA_${RUN_DATE}.csv "$FINAL_CSV"
  
  #Alternate way
  #mkdir $FINAL_CSV
  #hdfs dfs -cat ${HDFS_OUTPUT}/part* > "${FINAL_CSV}/DATA_${RUN_DATE}.csv"

else
  echo " Detected: Running OUTSIDE container (Ubuntu host)"

  source /mnt/c/users/'siddhesh bari'/DP/spark-apps/S260100/shell/env.sh "$ENV"

  echo "Uploading to HDFS via docker exec"
  docker exec hdfs-namenode hdfs dfs -rm -r -f ${HDFS_INPUT}
  docker exec hdfs-namenode hdfs dfs -mkdir -p ${HDFS_INPUT}
  docker exec hdfs-namenode hdfs dfs -put "${COLLECTION_PATH}" ${HDFS_INPUT}

  echo "Submitting Spark job via docker exec"
  docker exec hdfs-namenode hadoop fs -chmod -R 777 "$PERMISSION"
  docker exec spark-master spark-submit /opt/spark-apps/S260100/scripts/etl_job.py "$RUN_DATE" "$ENV" "$HDFS_INPUT" "$HDFS_OUTPUT"

  echo "Merging HDFS output to host shared folder"
  #One way (preferred)
  docker exec hdfs-namenode hdfs dfs -getmerge ${HDFS_OUTPUT}/part* "${FINAL_CSV}/DATA_${RUN_DATE}.csv"

  #Alternate way
  #docker exec hdfs-namenode mkdir "$FINAL_CSV"
  #docker exec hdfs-namenode hdfs dfs -cat ${HDFS_OUTPUT}/part* > "${FINAL_CSV}/DATA_${RUN_DATE}.csv"
  
fi

echo "Done."
