import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window

def main(date,env,input,output):
    #Create Spark Session
    spark=SparkSession.builder.appName("ETL").master("spark://spark-master:7077").getOrCreate()

    #Create DataFrame (Extract)
    file_name='Big_Sales_Data.csv' if env=='prod' else 'Big_Sales_Data_dev.csv'
    df=spark.read.format('CSV').option('inferSchema',True).option('Header',True).load(f"hdfs://hdfs-namenode:9000{input}/{file_name}")

    #Transform
    df=df.distinct()
    df=df.withColumn('Item_Fat_Content',regexp_replace(col('Item_Fat_Content'),'LF','Low Fat'))\
     .withColumn('Item_Fat_Content',regexp_replace(col('Item_Fat_Content'),'low fat','Low Fat'))\
     .withColumn('Item_Fat_Content',regexp_replace(col('Item_Fat_Content'),'reg','Regular'))
    df=df.withColumn('Outlet_Type',when(col('Outlet_Type').isin('Supermarket Type1','Supermarket Type2','Supermarket Type3'),'Supermarket')\
     .otherwise('Grocery Store'))
    #df=df.groupBy('Outlet_Identifier','Outlet_Type').pivot('Item_Fat_Content').agg(round(sum(col('Item_Outlet_Sales')),2))\
    # .sort('Outlet_Identifier')

    #Load (Save)
    df.write.mode('overwrite').format('CSV').option('path',f'hdfs://hdfs-namenode:9000{output}').save()

    #End Spark Session
    spark.stop()


if __name__ == "__main__":
    if len(sys.argv) < 5:
        print("Incorrect number of inputs")
        sys.exit(1)
    main(sys.argv[1],sys.argv[2],sys.argv[3],sys.argv[4])
