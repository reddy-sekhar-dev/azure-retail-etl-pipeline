spark.conf.set(
    "fs.azure.account.key.retailetlstorage12345.dfs.core.windows.net",
    "REPLACE_WITH_STORAGE_KEY"
)

customer_df = spark.read.format("delta").load(
    "abfss://retail@retailetlstorage12345.dfs.core.windows.net/silver/customer"
)

product_df = spark.read.format("delta").load(
    "abfss://retail@retailetlstorage12345.dfs.core.windows.net/silver/product"
)

sales_df = spark.read.format("delta").load(
    "abfss://retail@retailetlstorage12345.dfs.core.windows.net/silver/sales"
)
gold_df = spark.sql("""
SELECT
    s.order_id,
    s.order_date,
    s.customer_id,
    c.email,
    c.gender,
    c.region,
    c.loyalty_tier,
    s.product_id,
    p.product_name,
    p.category,
    s.quantity,
    s.unit_price,
    s.discount_applied,
    s.delivery_status,
    s.payment_method
FROM sales s
LEFT JOIN customer c
ON s.customer_id = c.customer_id
LEFT JOIN product p
ON s.product_id = p.product_id
""")

display(gold_df)
from pyspark.sql.functions import col

gold_df = gold_df.filter(
    col("quantity").rlike("^[0-9]+$")
).filter(
    col("unit_price").rlike("^[0-9]+(\\.[0-9]+)?$")
).filter(
    col("discount_applied").rlike("^[0-9]+(\\.[0-9]+)?$")
)

gold_df = gold_df.withColumn("quantity", col("quantity").cast("int")) \
    .withColumn("unit_price", col("unit_price").cast("double")) \
    .withColumn("discount_applied", col("discount_applied").cast("double"))

gold_df = gold_df.withColumn(
    "total_revenue",
    col("quantity") * col("unit_price") * (1 - col("discount_applied"))
)

display(gold_df)
from pyspark.sql.functions import col

gold_df = gold_df.withColumn(
    "total_revenue",
    col("quantity").cast("float") * col("unit_price").cast("float") * (1 - col("discount_applied").cast("float"))
)

display(gold_df) 
from pyspark.sql.functions import sum

region_revenue = gold_df.groupBy("region").agg(
    sum("total_revenue").alias("total_revenue")
)

display(region_revenue)
region_revenue.write \
    .format("delta") \
    .mode("overwrite") \
    .save("abfss://retail@retailetlstorage12345.dfs.core.windows.net/gold/region_revenue")
from pyspark.sql.functions import sum

top_products = gold_df.groupBy(
    "product_name"
).agg(
    sum("quantity").alias("total_quantity")
).orderBy(
    col("total_quantity").desc()
)

display(top_products)
top_products.write \
    .format("delta") \
    .mode("overwrite") \
    .save("abfss://retail@retailetlstorage12345.dfs.core.windows.net/gold/top_products")
customer_revenue = gold_df.groupBy(
    "customer_id",
    "email"
).agg(
    sum("total_revenue").alias("customer_revenue")
).orderBy(
    col("customer_revenue").desc()
)

display(customer_revenue)

customer_revenue.write \
    .format("delta") \
    .mode("overwrite") \
    .save("abfss://retail@retailetlstorage12345.dfs.core.windows.net/gold/customer_revenue")
payment_summary = gold_df.groupBy(
    "payment_method"
).count()

display(payment_summary)
payment_summary.write \
    .format("delta") \
    .mode("overwrite") \
    .save("abfss://retail@retailetlstorage12345.dfs.core.windows.net/gold/payment_summary")
delivery_summary = gold_df.groupBy(
    "delivery_status"
).count()

display(delivery_summary)
delivery_summary.write \
    .format("delta") \
    .mode("overwrite") \
    .save("abfss://retail@retailetlstorage12345.dfs.core.windows.net/gold/delivery_summary")