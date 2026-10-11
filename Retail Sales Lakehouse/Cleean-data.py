# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE CATALOG IF NOT EXISTS retail;
# MAGIC CREATE SCHEMA IF NOT EXISTS retail.bronze;
# MAGIC CREATE SCHEMA IF NOT EXISTS retail.silver;
# MAGIC CREATE SCHEMA IF NOT EXISTS retail.gold;
# MAGIC CREATE VOLUME IF NOT EXISTS retail.bronze.raw_files;

# COMMAND ----------

display(dbutils.fs.ls("/Volumes/retail/bronze/raw_files/"))

# COMMAND ----------

from pyspark.sql import functions as F

base = "/Volumes/retail/bronze/raw_files/"
files = {
    "orders": "olist_orders_dataset.csv",
    "order_items": "olist_order_items_dataset.csv",
    "customers": "olist_customers_dataset.csv",
    "products": "olist_products_dataset.csv",
    "payments": "olist_order_payments_dataset.csv",
}

for name, f in files.items():
    df = (spark.read.option("header", True).option("inferSchema", True).csv(base + f)
          .withColumn("_ingested_at", F.current_timestamp())
          .withColumn("_source_file", F.lit(f)))
    df.write.format("delta").mode("overwrite").saveAsTable(f"retail.bronze.{name}")

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Orders: fix types, drop duplicates, filter bad rows
orders = (spark.table("retail.bronze.orders")
    .dropDuplicates(["order_id"])
    .withColumn("order_purchase_timestamp", F.to_timestamp("order_purchase_timestamp"))
    .withColumn("order_delivered_customer_date", F.to_timestamp("order_delivered_customer_date"))
    .filter(F.col("order_id").isNotNull())
    .withColumn("order_date", F.to_date("order_purchase_timestamp"))
    .withColumn("delivery_days",
        F.datediff("order_delivered_customer_date", "order_purchase_timestamp")))
orders.write.format("delta").mode("overwrite").saveAsTable("retail.silver.orders")

# Items: cast price, compute line total
items = (spark.table("retail.bronze.order_items")
    .withColumn("price", F.col("price").cast("decimal(10,2)"))
    .withColumn("freight_value", F.col("freight_value").cast("decimal(10,2)"))
    .withColumn("line_total", F.col("price") + F.col("freight_value")))
items.write.format("delta").mode("overwrite").saveAsTable("retail.silver.order_items")

# Customers and products: dedupe and clean
(spark.table("retail.bronze.customers").dropDuplicates(["customer_id"])
    .write.format("delta").mode("overwrite").saveAsTable("retail.silver.customers"))

(spark.table("retail.bronze.products").dropDuplicates(["product_id"])
    .fillna({"product_category_name": "unknown"})
    .write.format("delta").mode("overwrite").saveAsTable("retail.silver.products"))

# Payments: aggregate to one row per order (groupBy + agg)
(spark.table("retail.bronze.payments")
    .groupBy("order_id")
    .agg(F.sum("payment_value").alias("total_paid"),
         F.countDistinct("payment_type").alias("payment_methods"))
    .write.format("delta").mode("overwrite").saveAsTable("retail.silver.payments"))

# COMMAND ----------

o = spark.table("retail.silver.orders")
c = spark.table("retail.silver.customers")
o.join(c, "customer_id", "inner").select("order_id", "customer_city").show(5)
# Try "left", "left_anti" (orders with no customer) too

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 1. Join + aggregate: revenue by customer state
# MAGIC SELECT c.customer_state, ROUND(SUM(i.line_total),2) AS revenue
# MAGIC FROM retail.silver.order_items i
# MAGIC JOIN retail.silver.orders o ON i.order_id = o.order_id
# MAGIC JOIN retail.silver.customers c ON o.customer_id = c.customer_id
# MAGIC GROUP BY c.customer_state
# MAGIC ORDER BY revenue DESC;
# MAGIC
# MAGIC -- 2. Window function: rank products within each category by revenue
# MAGIC SELECT * FROM (
# MAGIC   SELECT p.product_category_name, i.product_id,
# MAGIC          SUM(i.price) AS revenue,
# MAGIC          RANK() OVER (PARTITION BY p.product_category_name ORDER BY SUM(i.price) DESC) AS rnk
# MAGIC   FROM retail.silver.order_items i
# MAGIC   JOIN retail.silver.products p ON i.product_id = p.product_id
# MAGIC   GROUP BY p.product_category_name, i.product_id
# MAGIC ) WHERE rnk <= 3;
# MAGIC
# MAGIC -- 3. Window function: running total of daily revenue
# MAGIC SELECT order_date,
# MAGIC        SUM(daily_rev) OVER (ORDER BY order_date) AS running_revenue
# MAGIC FROM (
# MAGIC   SELECT o.order_date, SUM(i.price) AS daily_rev
# MAGIC   FROM retail.silver.orders o JOIN retail.silver.order_items i USING (order_id)
# MAGIC   GROUP BY o.order_date
# MAGIC );
# MAGIC
# MAGIC -- 4. LAG: month-over-month growth
# MAGIC WITH monthly AS (
# MAGIC   SELECT date_trunc('month', order_date) AS month, SUM(i.price) AS rev
# MAGIC   FROM retail.silver.orders o JOIN retail.silver.order_items i USING (order_id)
# MAGIC   GROUP BY 1)
# MAGIC SELECT month, rev, LAG(rev) OVER (ORDER BY month) AS prev_rev,
# MAGIC        ROUND((rev - LAG(rev) OVER (ORDER BY month)) / LAG(rev) OVER (ORDER BY month) * 100, 1) AS growth_pct
# MAGIC FROM monthly;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY retail.silver.orders;
# MAGIC UPDATE retail.silver.orders SET order_status = 'test' WHERE order_id IN (SELECT order_id FROM retail.silver.orders LIMIT 5);
# MAGIC SELECT * FROM retail.silver.orders VERSION AS OF 0 LIMIT 5;   -- before the update
# MAGIC RESTORE TABLE retail.silver.orders TO VERSION AS OF 0;         -- undo it

# COMMAND ----------

from delta.tables import DeltaTable

updates = spark.createDataFrame(
    [("ORDER_NEW_1", "cust_1", "delivered"),
     ("<an existing order_id>", "cust_2", "canceled")],
    ["order_id", "customer_id", "order_status"])
updates.createOrReplaceTempView("order_updates")

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO retail.silver.orders t
# MAGIC USING order_updates s ON t.order_id = s.order_id
# MAGIC WHEN MATCHED THEN UPDATE SET t.order_status = s.order_status
# MAGIC WHEN NOT MATCHED THEN INSERT (order_id, customer_id, order_status) VALUES (s.order_id, s.customer_id, s.order_status);

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE retail.gold.dim_customer AS
# MAGIC SELECT customer_id AS customer_key, customer_unique_id, customer_city, customer_state
# MAGIC FROM retail.silver.customers;
# MAGIC
# MAGIC CREATE OR REPLACE TABLE retail.gold.dim_product AS
# MAGIC SELECT product_id AS product_key, product_category_name AS category,
# MAGIC        product_weight_g, product_photos_qty
# MAGIC FROM retail.silver.products;
# MAGIC
# MAGIC CREATE OR REPLACE TABLE retail.gold.dim_date AS
# MAGIC SELECT DISTINCT order_date AS date_key,
# MAGIC        year(order_date) AS year, month(order_date) AS month,
# MAGIC        quarter(order_date) AS quarter, dayofweek(order_date) AS day_of_week
# MAGIC FROM retail.silver.orders;
# MAGIC
# MAGIC CREATE OR REPLACE TABLE retail.gold.fact_sales AS
# MAGIC SELECT i.order_id, i.order_item_id,
# MAGIC        o.customer_id AS customer_key,
# MAGIC        i.product_id AS product_key,
# MAGIC        o.order_date AS date_key,
# MAGIC        o.order_status, o.delivery_days,
# MAGIC        i.price, i.freight_value, i.line_total
# MAGIC FROM retail.silver.order_items i
# MAGIC JOIN retail.silver.orders o ON i.order_id = o.order_id;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW retail.gold.v_sales_metrics AS
# MAGIC SELECT d.year, d.month, c.customer_state, p.category,
# MAGIC        COUNT(DISTINCT f.order_id)                              AS total_orders,
# MAGIC        SUM(f.price)                                            AS gross_revenue,
# MAGIC        SUM(f.price) / COUNT(DISTINCT f.order_id)               AS avg_order_value,
# MAGIC        AVG(f.delivery_days)                                    AS avg_delivery_days
# MAGIC FROM retail.gold.fact_sales f
# MAGIC JOIN retail.gold.dim_date d     ON f.date_key = d.date_key
# MAGIC JOIN retail.gold.dim_customer c ON f.customer_key = c.customer_key
# MAGIC JOIN retail.gold.dim_product p  ON f.product_key = p.product_key
# MAGIC WHERE f.order_status = 'delivered'
# MAGIC GROUP BY d.year, d.month, c.customer_state, p.category;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Document your assets
# MAGIC COMMENT ON TABLE retail.gold.fact_sales IS 'One row per order item. Source of truth for sales.';
# MAGIC ALTER TABLE retail.gold.dim_customer ALTER COLUMN customer_unique_id SET TAGS ('pii' = 'true');
# MAGIC
# MAGIC -- Permissions (the pattern: analysts read Gold only)
# MAGIC GRANT USE CATALOG ON CATALOG retail TO `account users`;
# MAGIC GRANT USE SCHEMA, SELECT ON SCHEMA retail.gold TO `account users`;
# MAGIC -- Bronze and Silver stay restricted to engineers

# COMMAND ----------

# MAGIC %sql
# MAGIC COMMENT ON TABLE retail.gold.fact_sales IS 'One row per order item. Source of truth for sales.';
# MAGIC COMMENT ON VIEW retail.gold.v_sales_metrics IS 'Official business metrics: delivered orders only, revenue = item price.';
# MAGIC ALTER TABLE retail.gold.dim_customer ALTER COLUMN customer_unique_id SET TAGS ('pii' = 'true');

# COMMAND ----------

# MAGIC %sql
# MAGIC COMMENT ON TABLE retail.gold.fact_sales IS 'One row per order item. Source of truth for sales.';
# MAGIC COMMENT ON TABLE retail.gold.dim_customer IS 'One row per customer with city and state.';
# MAGIC COMMENT ON TABLE retail.gold.dim_product IS 'One row per product with category.';
# MAGIC COMMENT ON TABLE retail.gold.dim_date IS 'One row per calendar date with year, month and quarter.';
# MAGIC COMMENT ON VIEW retail.gold.v_sales_metrics IS 'Official business metrics. Delivered orders only, revenue = item price.';
# MAGIC ALTER TABLE retail.gold.dim_customer ALTER COLUMN customer_unique_id SET TAGS ('pii' = 'true');

# COMMAND ----------

# MAGIC %sql
# MAGIC GRANT USE CATALOG ON CATALOG retail TO `account users`;
# MAGIC GRANT USE SCHEMA, SELECT ON SCHEMA retail.gold TO `account users`;

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW GRANTS ON SCHEMA retail.gold;

# COMMAND ----------

from pyspark.sql import functions as F

fact = spark.table("retail.gold.fact_sales")
items = spark.table("retail.silver.order_items")

assert fact.count() == items.count(), "fact_sales row count does not match order_items"
assert fact.filter(F.col("order_id").isNull()).count() == 0, "null order_id in fact_sales"
assert fact.filter(F.col("price") < 0).count() == 0, "negative prices found"

print("All quality checks passed")

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES IN retail.bronze;
# MAGIC SHOW TABLES IN retail.silver;
# MAGIC SHOW TABLES IN retail.gold;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT table_schema, table_name, table_type
# MAGIC FROM retail.information_schema.tables
# MAGIC WHERE table_schema IN ('bronze','silver','gold')
# MAGIC ORDER BY table_schema, table_name;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM retail.gold.fact_sales LIMIT 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM retail.gold.fact_sales;

# COMMAND ----------

for schema in ["bronze", "silver", "gold"]:
    for t in spark.sql(f"SHOW TABLES IN retail.{schema}").collect():
        if not t.isTemporary:
            name = f"retail.{schema}.{t.tableName}"
            try:
                print(name, spark.table(name).count())
            except Exception as e:
                print(name, "error:", e)

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE DETAIL retail.gold.fact_sales;
# MAGIC DESCRIBE HISTORY retail.gold.fact_sales;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT c.customer_unique_id AS user_id,
# MAGIC        c.customer_city,
# MAGIC        c.customer_state,
# MAGIC        COUNT(DISTINCT f.order_id) AS total_orders,
# MAGIC        ROUND(SUM(f.price), 2) AS total_spent
# MAGIC FROM retail.gold.fact_sales f
# MAGIC JOIN retail.gold.dim_customer c ON f.customer_key = c.customer_key
# MAGIC WHERE f.order_status = 'delivered'
# MAGIC GROUP BY c.customer_unique_id, c.customer_city, c.customer_state
# MAGIC ORDER BY total_spent DESC
# MAGIC LIMIT 20;

# COMMAND ----------



# COMMAND ----------

import mlflow

with mlflow.start_run():
    mlflow.log_param("model", "random_forest")
    mlflow.log_metric("accuracy", 0.91)