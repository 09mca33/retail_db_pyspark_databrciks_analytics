# Databricks notebook source
from pyspark.sql.types import *
from pyspark.sql import functions as F

# COMMAND ----------

order_item_schema = StructType(
                    [StructField("order_item_id", IntegerType(), True) ,
                     StructField("order_item_order_id", IntegerType(), True) ,
                     StructField("order_item_product_id", IntegerType(), True) ,
                     StructField("order_item_quantity", IntegerType(), True) ,
                     StructField("order_item_subtotal", FloatType(), True) ,
                     StructField("order_item_product_price", FloatType(), True) 
                    ]
                    )
order_items= spark.read \
            .format('csv') \
            .option('sep',',') \
            .option('header','false') \
            .option('Path','/FileStore/tables/order_items.csv') \
            .schema(order_item_schema) \
            .load()

display(order_items)


# COMMAND ----------

orders_column = 'order_id int, order_date string, order_customer_id int, order_status string'

orders= spark.read \
            .format('csv') \
            .option('sep',',') \
            .option('header','false') \
            .option('Path','/FileStore/tables/orders.csv') \
            .schema(orders_column) \
            .load()

# display(orders)
# display(order_items)

# COMMAND ----------

#orders = orders.repartition(4)

orders.rdd.getNumPartitions()
#orders.write.mode("overwrite").option("format","parquet").saveAsTable("retail_db_ingestion.orders")

# COMMAND ----------

order_items = order_items.repartition(4)
order_items.write.mode("overwrite").option("format","parquet").saveAsTable("retail_db_ingestion.order_items")

# COMMAND ----------

order_items.rdd.getNumPartitions()

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select * from retail_db_ingestion.order_items

# COMMAND ----------

join_cnd = (orders.order_id == order_items.order_item_order_id)

result = order_items.join(orders, join_cnd,how='inner').select("order_status","order_date","order_item_subtotal") \
                                                        .groupBy("order_status").agg(F.sum(F.col('order_item_subtotal')).alias("total_rev"))
#result.persist()
display(result)

# COMMAND ----------

spark.conf.set("spark.sql.autoBroadcastJoinThreshold","-1")

# COMMAND ----------

spark.conf.set('spark.sql.suffle.partitions','4')
# spark.conf.get('spark.sql.suffle.partitions')

# COMMAND ----------

print(spark.sparkContext.defaultParallelism)
# print(spark.sql.suffle.partitions)


# COMMAND ----------

orders.rdd.getNumPartitions()

# COMMAND ----------

orders = orders.repartition(4)

# COMMAND ----------

orders_tbl = spark.read.table("retail_db_ingestion.orders")
order_items_tbl = spark.read.table("retail_db_ingestion.order_items")

display(orders_tbl)

# COMMAND ----------

join_cnd = (orders_tbl.order_id == order_items_tbl.order_item_order_id)
orders_join = orders_tbl.join(order_items_tbl,join_cnd,'inner')
display(orders_join)

# COMMAND ----------

.write.bucketBy(10, "num1").saveAsTable("bucketed_large_table_1")

# COMMAND ----------

# MAGIC %sql
# MAGIC create table retail_db_cleanse.orders as 
# MAGIC select * from retail_db_ingestion.orders

# COMMAND ----------



# COMMAND ----------

orders_tbl.write \
      .bucketBy(4,'order_id') \
      .format("parquet") \
      .option("compression","snappy") \
      .option("mode","overwrite") \
      .option("path",'/FileStore/tables/bucket/') \
      .save()

# COMMAND ----------

orders_tbl.coalesce(1).write.bucketBy(4,'order_id').mode("overwrite").saveAsTable("retail_db_cleanse.orders")

# COMMAND ----------

orders_tbl.coalesce(1) \
            .write \
          .bucktedBy(4,'order_id') \
          .mode('overwride') \
          .option('format','parquet') \
          .saveAsTable('retail_db_cleanse.orders')


# COMMAND ----------

order_items_tbl.coalesce(1) \
          .bucktedBy(4,'order_item_order_id') \
          .mode('overwride') \
          .option('format','parquet') \
          .saveAsTable('retail_db_cleanse.order_items')

# COMMAND ----------

spark.conf.get('spark.sql.suffle.partitions','4')
