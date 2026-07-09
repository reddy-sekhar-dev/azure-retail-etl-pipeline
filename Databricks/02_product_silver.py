from pyspark.sql.functions import *
spark.conf.set(
    "fs.azure.account.key.retailetlstorage12345.dfs.core.windows.net",
    "REPLACE_WITH_STORAGE_KEY"
)
product_df = spark.read \
    .option("header", "true") \
    .csv("abfss://retail@retailetlstorage12345.dfs.core.windows.net/raw/product_info.csv")

display(product_df)

product_df.select(
    [count(when(col(c).isNull(), c)).alias(c) for c in product_df.columns]
).show()
print("Total Records :", product_df.count())
print("Distinct Records :", product_df.distinct().count())
product_df=product_df.dropDuplicates()

product_df = product_df.withColumn(
    "product_id",
    trim(col("product_id"))
)

product_df = product_df.withColumn(
    "product_name",
    trim(col("product_name"))
)

product_df = product_df.withColumn(
    "category",
    initcap(trim(col("category")))
)

product_df = product_df.withColumn(
    "supplier_code",
    trim(col("supplier_code"))
)

product_df = product_df.withColumn(
    "launch_date",
    to_date(col("launch_date"), "dd-MM-yy")
)

product_df = product_df.withColumn(
    "base_price",
    col("base_price").cast("double")
)

product_df = product_df.fillna({
    "category":"Unknown",
    "supplier_code":"Unknown"
})
display(product_df)

product_df.printSchema()

product_df.select("category").distinct().show()

product_df.select(
    [count(when(col(c).isNull(), c)).alias(c) for c in product_df.columns]
).show()
product_df.write \
    .format("delta") \
    .mode("overwrite") \
    .save("abfss://retail@retailetlstorage12345.dfs.core.windows.net/silver/product")
silver_product_df = spark.read \
    .format("delta") \
    .load("abfss://retail@retailetlstorage12345.dfs.core.windows.net/silver/product")

display(silver_product_df)