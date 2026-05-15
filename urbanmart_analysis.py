from pyspark.sql import SparkSession
from pyspark.sql.functions import col, desc, sum as spark_sum, count, hour

spark = SparkSession.builder.appName("UrbanMartAnalysis").getOrCreate()
base = "/assesment4/UrbanMart_Balanced_Dataset_MDS612"

transactions = spark.read.option("header", "true").option("inferSchema", "true").csv(base + "/transactions.csv")
products = spark.read.option("header", "true").option("inferSchema", "true").csv(base + "/products.csv")
events = spark.read.option("header", "true").option("inferSchema", "true").csv(base + "/web_app_events.csv")
sensors = spark.read.option("header", "true").option("inferSchema", "true").csv(base + "/iot_sensor_readings.csv")


""" print("=== TRANSACTIONS SCHEMA ===")
transactions.printSchema()
transactions.show(5, truncate=False)

print("=== PRODUCTS SCHEMA ===")
products.printSchema()
products.show(5, truncate=False)

print("=== EVENTS SCHEMA ===")
events.printSchema()
events.show(5, truncate=False)

print("=== SENSORS SCHEMA ===")
sensors.printSchema()
sensors.show(5, truncate=False) """


print("=== QUALITY CHECK 1: transactions with sales_amount null or negative ===")
q1 = transactions.filter(col("sales_amount").isNull() | (col("sales_amount") < 0))
q1_count = q1.count()
print(f" records with sales_amount null or negative: {q1_count}")


print("=== QUALITY CHECK 2: events with null product_id ===")
q2 = events.filter(col("product_id").isNull())
q2_count = q2.count()
print(f"events with null product_id: {q2_count}")
events.groupBy("event_type").agg(
    count("*").alias("total"),
    count("product_id").alias("with_product_id")
).orderBy(desc("total")).show()


print("=== Q1: Revenue by Product Category ===")
revenue_by_category = (
    transactions.join(products, "product_id")
    .groupBy("category")
    .agg(spark_sum("sales_amount").alias("total_revenue"))
    .orderBy(desc("total_revenue"))
)
revenue_by_category.show()

print("=== Q2: Online Event Type Frequency ===")
events.groupBy("event_type").count().orderBy(desc("count")).show()

print("=== Q3: Sensor Alerts by Store and Zone ===")
sensors.filter(col("status") != "normal") \
    .groupBy("store_id", "zone", "sensor_type", "status") \
    .count() \
    .orderBy(desc("count")) \
    .show(20)
    
    

output_base = "/assesment4/outputs"


# Dashboard Output 1 — Revenue by Category
revenue_by_category.coalesce(1).write.mode("overwrite") \
    .option("header", "true").csv(output_base + "/q1_revenue_by_category")

# Dashboard Output 2 — Event Type Frequency
events.groupBy("event_type").count().orderBy(desc("count")) \
    .coalesce(1).write.mode("overwrite") \
    .option("header", "true").csv(output_base + "/q2_event_frequency")

# Dashboard Output 3 — Sensor Anomalies
sensors.filter(col("status") != "normal") \
    .groupBy("store_id", "zone", "sensor_type", "status").count() \
    .orderBy(desc("count")) \
    .coalesce(1).write.mode("overwrite") \
    .option("header", "true").csv(output_base + "/q3_sensor_alerts")

# Dashboard Output 4 — Cart Abandonment Summary
cart_summary = events.groupBy("event_type").count().orderBy(desc("count"))
cart_summary.coalesce(1).write.mode("overwrite") \
    .option("header", "true").csv(output_base + "/q4_cart_summary")

print("=== All dashboard outputs exported successfully ===")




spark.stop()