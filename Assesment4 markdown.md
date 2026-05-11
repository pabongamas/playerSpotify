## check folder is in docker

root@1d941616c21d:/workspace/apps# ls
UrbanMart_Balanced_Dataset_MDS612  week10_pyspark_setup.py  week6_dataframe_analysis.py  week8_stream_console.py
week10_mllib_pipeline.py           week5_mnm_count.py       week7_spark_sql.py           week9_window_demo.py

this is the folder with datasets


## start NameNode

root@1d941616c21d:/workspace# /usr/local/bin/start-lab.sh
Formatting HDFS NameNode for first use...

MDS612 lab services started.
NameNode UI:    http://localhost:9870
DataNode UI:    http://localhost:9864
YARN UI:        http://localhost:8088
JobHistory UI:  http://localhost:19888
Spark History:  http://localhost:18080
Socket demo:    localhost:9999

673 DataNode
1109 ResourceManager
1222 NodeManager
1607 JobHistoryServer
2248 Jps
1656 HistoryServer
857 SecondaryNameNode
556 NameNode


## verify connection to haddoop , verify current directiory with info 

root@1d941616c21d:/workspace# hdfs dfs -ls /
Found 3 items
drwxrwxrwx   - root supergroup          0 2026-05-11 21:38 /data
drwxrwx---   - root supergroup          0 2026-05-11 21:37 /tmp
drwxrwxrwx   - root supergroup          0 2026-05-11 21:37 /tutorial


## create folder to move inside datasets ,from machine docker to haddop

root@1d941616c21d:/workspace/apps# hdfs dfs -mkdir /assesment4

## put folder with datasets in hadoop folder /assesment

root@1d941616c21d:/workspace/apps# hdfs dfs -put UrbanMart_Balanced_Dataset_MDS612 /assesment4

## verify files in hadoop folder


root@1d941616c21d:/workspace/apps# hdfs dfs -ls /assesment4
Found 1 items
drwxr-xr-x   - root supergroup          0 2026-05-11 22:06 /assesment4/UrbanMart_Balanced_Dataset_MDS612
root@1d941616c21d:/workspace/apps# hdfs dfs -ls /assesment4/UrbanMart_Balanced_Dataset_MDS612
Found 9 items
-rw-r--r--   1 root supergroup        884 2026-05-11 22:06 /assesment4/UrbanMart_Balanced_Dataset_MDS612/README.txt
-rw-r--r--   1 root supergroup      25280 2026-05-11 22:06 /assesment4/UrbanMart_Balanced_Dataset_MDS612/customers.csv
-rw-r--r--   1 root supergroup        835 2026-05-11 22:06 /assesment4/UrbanMart_Balanced_Dataset_MDS612/data_dictionary.csv
-rw-r--r--   1 root supergroup     602616 2026-05-11 22:06 /assesment4/UrbanMart_Balanced_Dataset_MDS612/iot_sensor_readings.csv
-rw-r--r--   1 root supergroup       3488 2026-05-11 22:06 /assesment4/UrbanMart_Balanced_Dataset_MDS612/products.csv
-rw-r--r--   1 root supergroup        196 2026-05-11 22:06 /assesment4/UrbanMart_Balanced_Dataset_MDS612/stores.csv
-rw-r--r--   1 root supergroup     481805 2026-05-11 22:06 /assesment4/UrbanMart_Balanced_Dataset_MDS612/transactions.csv
-rw-r--r--   1 root supergroup       1303 2026-05-11 22:06 /assesment4/UrbanMart_Balanced_Dataset_MDS612/urbanmart_starter_analysis.py
-rw-r--r--   1 root supergroup     816335 2026-05-11 22:06 /assesment4/UrbanMart_Balanced_Dataset_MDS612/web_app_events.csv

## check files were uploaded , its verified using cat showing head limiting 5 rows

root@1d941616c21d:/workspace/apps# hdfs dfs -cat /assesment4/UrbanMart_Balanced_Dataset_MDS612/README.txt | head -n 5
UrbanMart Balanced Dataset for MDS612 Group Assignment

This dataset is synthetic and designed for beginner-to-intermediate Spark/HDFS analysis.
It is not real customer data.

root@1d941616c21d:/workspace/apps# hdfs dfs -head /assesment4/UrbanMart_Balanced_Dataset_MDS612/customers.csv | head -n 5
customer_id,age_group,segment,loyalty_member,home_suburb
C0001,45-54,Student,Y,Blacktown
C0002,55+,Regular,Y,Sydney CBD
C0003,18-24,Regular,Y,Parramatta
C0004,18-24,Family,N,Hurstville

root@1d941616c21d:/workspace/apps# hdfs dfs -head /assesment4/UrbanMart_Balanced_Dataset_MDS612/data_dictionary.csv | head -n 5
file,column,description
transactions.csv,transaction_id,Unique transaction identifier
transactions.csv,timestamp,Transaction time
transactions.csv,store_id,Store identifier
transactions.csv,customer_id,Customer identifier; a small number are missing intentionally

root@1d941616c21d:/workspace/apps# hdfs dfs -head /assesment4/UrbanMart_Balanced_Dataset_MDS612/iot_sensor_readings.csv | head -n 5
reading_id,timestamp,store_id,zone,sensor_type,value,unit,status
R00001,2026-03-26 16:43:00,S003,aisle_1,footfall,8,count,normal
R00002,2026-03-02 16:37:00,S001,storage,humidity,54.06,%,normal
R00003,2026-03-06 22:14:00,S001,entrance,footfall,23,count,normal
R00004,2026-03-20 03:43:00,S001,checkout,humidity,54.66,%,normal

root@1d941616c21d:/workspace/apps# hdfs dfs -head /assesment4/UrbanMart_Balanced_Dataset_MDS612/products.csv | head -n 5
product_id,product_name,category,unit_price,supplier_region
P001,Electronics Item 001,Electronics,145.38,NSW
P002,Pantry Item 002,Pantry,25.16,NSW
P003,Pantry Item 003,Pantry,31.01,QLD
P004,Fresh Food Item 004,Fresh Food,17.39,QLD

root@1d941616c21d:/workspace/apps# hdfs dfs -head /assesment4/UrbanMart_Balanced_Dataset_MDS612/stores.csv | head -n 5
store_id,store_name,suburb,store_type,floor_area_sqm
S001,UrbanMart Central,Sydney CBD,metro,620
S002,UrbanMart West,Parramatta,suburban,850
S003,UrbanMart North,Chatswood,shopping centre,760

root@1d941616c21d:/workspace/apps# hdfs dfs -head /assesment4/UrbanMart_Balanced_Dataset_MDS612/transactions.csv | head -n 5
transaction_id,timestamp,store_id,customer_id,product_id,quantity,unit_price,discount_rate,payment_method,sales_amount
T00001,2026-03-17 22:22:00,S001,C0296,P062,3,31.71,0,cash,95.13
T00002,2026-03-17 19:33:00,S001,C0306,P077,3,22.52,0.1,gift_card,60.8
T00003,2026-03-06 03:10:00,S002,C0327,P009,1,56.15,0.15,cash,47.73
T00004,2026-03-21 02:17:00,S003,C0004,P048,2,21.13,0.05,card,40.15


root@1d941616c21d:/workspace/apps# hdfs dfs -head /assesment4/UrbanMart_Balanced_Dataset_MDS612/web_app_events.csv | head -n 5
event_id,timestamp,session_id,customer_id,store_id,channel,device_type,event_type,page,product_id
E00001,2026-03-30 02:11:33,SESS1913,C0574,S002,mobile_app,phone,checkout_start,promotion,
E00002,2026-03-04 23:47:00,SESS2352,C0459,S003,web,desktop,view,support,P061
E00003,2026-03-09 22:51:48,SESS0998,C0172,S003,mobile_app,phone,search,support,
E00004,2026-03-16 05:59:01,SESS0180,C0197,S001,mobile_app,desktop,checkout_start,product,


## create python script to analysis

 create in machine using jupiter notebook or visual studio code , save , and copy to docker in folder where are the files to process

PS C:\Users\jhoni\Downloads> docker cp .\urbanmart_analysis.py 1d941616c21d:/workspace/apps/
Successfully copied 2.56kB to 1d941616c21d:/workspace/apps/

this process has to be done in the folder where the python script is saved 
,was here PS C:\Users\jhoni\Downloads> and the file will be copied at folder /apps in the docker container 
Path : 1d941616c21d:/workspace/apps/

## verify file is at folder /apps 

root@1d941616c21d:/workspace/apps# ls
UrbanMart_Balanced_Dataset_MDS612  urbanmart_analysis.py  week10_mllib_pipeline.py  week10_pyspark_setup.py  week5_mnm_count.py  week6_dataframe_analysis.py  week7_spark_sql.py  week8_stream_console.py  week9_window_demo.py



## run  spark-submit /workspace/apps/urbanmart_analysis.py to execute script with spark 

in this moment this is the code in the script 

transactions = spark.read.option("header", "true").option("inferSchema", "true").csv(base + "/transactions.csv")
products = spark.read.option("header", "true").option("inferSchema", "true").csv(base + "/products.csv")
events = spark.read.option("header", "true").option("inferSchema", "true").csv(base + "/web_app_events.csv")
sensors = spark.read.option("header", "true").option("inferSchema", "true").csv(base + "/iot_sensor_readings.csv")


print("=== TRANSACTIONS SCHEMA ===")
transactions.printSchema()
transactions.show(5)

print("=== PRODUCTS SCHEMA ===")
products.printSchema()
products.show(5)

print("=== EVENTS SCHEMA ===")
events.printSchema()
events.show(5)

print("=== SENSORS SCHEMA ===")
sensors.printSchema()
sensors.show(5)



we will see this info detailed (tables , with datatypes,records) with the limit of 5 rows

=== TRANSACTIONS SCHEMA ===


root
 |-- transaction_id: string (nullable = true)
 |-- timestamp: timestamp (nullable = true)
 |-- store_id: string (nullable = true)
 |-- customer_id: string (nullable = true)
 |-- product_id: string (nullable = true)
 |-- quantity: integer (nullable = true)
 |-- unit_price: double (nullable = true)
 |-- discount_rate: double (nullable = true)
 |-- payment_method: string (nullable = true)
 |-- sales_amount: double (nullable = true)

 +--------------+-------------------+--------+-----------+----------+--------+----------+-------------+--------------+------------+
|transaction_id|          timestamp|store_id|customer_id|product_id|quantity|unit_price|discount_rate|payment_method|sales_amount|
+--------------+-------------------+--------+-----------+----------+--------+----------+-------------+--------------+------------+
|        T00001|2026-03-17 22:22:00|    S001|      C0296|      P062|       3|     31.71|          0.0|          cash|       95.13|
|        T00002|2026-03-17 19:33:00|    S001|      C0306|      P077|       3|     22.52|          0.1|     gift_card|        60.8|
|        T00003|2026-03-06 03:10:00|    S002|      C0327|      P009|       1|     56.15|         0.15|          cash|       47.73|
|        T00004|2026-03-21 02:17:00|    S003|      C0004|      P048|       2|     21.13|         0.05|          card|       40.15|
|        T00005|2026-03-24 20:54:00|    S002|      C0187|      P008|       1|    144.88|          0.0| mobile_wallet|      144.88|
+--------------+-------------------+--------+-----------+----------+--------+----------+-------------+--------------+------------+


=== PRODUCTS SCHEMA ===
root
 |-- product_id: string (nullable = true)
 |-- product_name: string (nullable = true)
 |-- category: string (nullable = true)
 |-- unit_price: double (nullable = true)
 |-- supplier_region: string (nullable = true)


+----------+--------------------+-----------+----------+---------------+
|product_id|        product_name|   category|unit_price|supplier_region|
+----------+--------------------+-----------+----------+---------------+
|      P001|Electronics Item 001|Electronics|    145.38|            NSW|
|      P002|     Pantry Item 002|     Pantry|     25.16|            NSW|
|      P003|     Pantry Item 003|     Pantry|     31.01|            QLD|
|      P004| Fresh Food Item 004| Fresh Food|     17.39|            QLD|
|      P005|     Pantry Item 005|     Pantry|     16.88|       Overseas|
+----------+--------------------+-----------+----------+---------------+


=== EVENTS SCHEMA ===

root
 |-- event_id: string (nullable = true)
 |-- timestamp: timestamp (nullable = true)
 |-- session_id: string (nullable = true)
 |-- customer_id: string (nullable = true)
 |-- store_id: string (nullable = true)
 |-- channel: string (nullable = true)
 |-- device_type: string (nullable = true)
 |-- event_type: string (nullable = true)
 |-- page: string (nullable = true)
 |-- product_id: string (nullable = true)



 +--------+-------------------+----------+-----------+--------+----------+-----------+--------------+---------+----------+
|event_id|          timestamp|session_id|customer_id|store_id|   channel|device_type|    event_type|     page|product_id|
+--------+-------------------+----------+-----------+--------+----------+-----------+--------------+---------+----------+
|  E00001|2026-03-30 02:11:33|  SESS1913|      C0574|    S002|mobile_app|      phone|checkout_start|promotion|      NULL|
|  E00002|2026-03-04 23:47:00|  SESS2352|      C0459|    S003|       web|    desktop|          view|  support|      P061|
|  E00003|2026-03-09 22:51:48|  SESS0998|      C0172|    S003|mobile_app|      phone|        search|  support|      NULL|
|  E00004|2026-03-16 05:59:01|  SESS0180|      C0197|    S001|mobile_app|    desktop|checkout_start|  product|      NULL|
|  E00005|2026-03-02 09:38:46|  SESS0152|      C0220|    S002|       web|    desktop|   add_to_cart|  support|      P023|
+--------+-------------------+----------+-----------+--------+----------+-----------+--------------+---------+----------+


=== SENSORS SCHEMA ===

root
 |-- reading_id: string (nullable = true)
 |-- timestamp: timestamp (nullable = true)
 |-- store_id: string (nullable = true)
 |-- zone: string (nullable = true)
 |-- sensor_type: string (nullable = true)
 |-- value: double (nullable = true)
 |-- unit: string (nullable = true)
 |-- status: string (nullable = true)


 +----------+-------------------+--------+--------+-----------+-----+-----+------+
|reading_id|          timestamp|store_id|    zone|sensor_type|value| unit|status|
+----------+-------------------+--------+--------+-----------+-----+-----+------+
|    R00001|2026-03-26 16:43:00|    S003| aisle_1|   footfall|  8.0|count|normal|
|    R00002|2026-03-02 16:37:00|    S001| storage|   humidity|54.06|    %|normal|
|    R00003|2026-03-06 22:14:00|    S001|entrance|   footfall| 23.0|count|normal|
|    R00004|2026-03-20 03:43:00|    S001|checkout|   humidity|54.66|    %|normal|
|    R00005|2026-03-07 22:49:00|    S002|entrance|   humidity|64.79|    %|normal|