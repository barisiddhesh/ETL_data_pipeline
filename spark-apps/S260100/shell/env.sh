#!/bin/bash

ENV="$1"
RUN_DATE=$(date +%F)
PERMISSION="/etl"

if [ "$ENV" == "prod" ]; then
  echo 'In production'
  COLLECTION_PATH="/opt/spark-apps/S260100/input/Big_Sales_Data.csv"
  HDFS_INPUT="/etl/input"
  HDFS_OUTPUT="/etl/output/${RUN_DATE}_PROD"
  FINAL_CSV="/opt/spark-apps/S260100/output/${RUN_DATE}_PROD"

else
  echo 'In development'
  COLLECTION_PATH="/opt/spark-apps/S260100/input/Big_Sales_Data_dev.csv"
  HDFS_INPUT="/etl/input"
  HDFS_OUTPUT="/etl/output/${RUN_DATE}_DEV"
  FINAL_CSV="/opt/spark-apps/S260100/output/${RUN_DATE}_DEV"
fi
