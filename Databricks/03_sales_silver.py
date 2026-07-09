from pyspark.sql.functions import *
spark.conf.set(
    "fs.azure.account.key.retailetlstorage12345.dfs.core.windows.net",
    "REPLACE_WITH_STORAGE_KEY"
)
sales_df = spark.read \
    .option("header", "true") \
    .csv("abfss://retail@retailetlstorage12345.dfs.core.windows.net/raw/sales_data.csv")

display(sales_df)
sales_df.select(
    [count(when(col(c).isNull(), c)).alias(c) for c in sales_df.columns]
).show()
print("Total Records :", sales_df.count())
print("Distinct Records :", sales_df.distinct().count())
sales_df = sales_df.dropDuplicates()

sales_df = sales_df.withColumn("order_id", trim(col("order_id"))) \
    .withColumn("customer_id", trim(col("customer_id"))) \
    .withColumn("product_id", trim(col("product_id"))) \
    .withColumn("quantity", trim(col("quantity"))) \
    .withColumn("unit_price", trim(col("unit_price"))) \
    .withColumn("order_date", trim(col("order_date"))) \
    .withColumn("delivery_status", initcap(trim(col("delivery_status")))) \
    .withColumn("payment_method", initcap(trim(col("payment_method")))) \
    .withColumn("region", initcap(trim(col("region")))) \
    .withColumn("discount_applied", trim(col("discount_applied")))

sales_df = sales_df.fillna({
    "delivery_status": "Pending",
    "payment_method": "Unknown",
    "region": "Unknown",
    "discount_applied": "0"
})

display(sales_df)
sales_df.write \
    .format("delta") \
    .mode("overwrite") \
    .save("abfss://retail@retailetlstorage12345.dfs.core.windows.net/silver/sales")