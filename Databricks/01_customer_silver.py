spark.conf.set(
    "fs.azure.account.key.retailetlstorage12345.dfs.core.windows.net",
    "REPLACE_WITH_STORAGE_KEY"
)
customer_df = spark.read \
    .option("header", "true") \
    .csv("abfss://retail@retailetlstorage12345.dfs.core.windows.net/raw/customer_info.csv")
customer_df.display()
display(dbutils.fs.ls("abfss://retail@retailetlstorage12345.dfs.core.windows.net/raw"))
from pyspark.sql.functions import *
customer_df.select(
    [count(when(col(c).isNull(), c)).alias(c) for c in customer_df.columns]
).show()
print("Total Records :", customer_df.count())
print("Distinct Records :", customer_df.distinct().count())
customer_df = customer_df.dropDuplicates()
from pyspark.sql.functions import *

# Remove duplicates
customer_df = customer_df.dropDuplicates()

# Trim spaces
customer_df = customer_df.withColumn("customer_id", trim(col("customer_id"))) \
    .withColumn("email", trim(col("email"))) \
    .withColumn("gender", trim(col("gender"))) \
    .withColumn("region", trim(col("region"))) \
    .withColumn("loyalty_tier", trim(col("loyalty_tier")))

# Standardize gender
customer_df = customer_df.withColumn(
    "gender",
    initcap(col("gender"))
)

# Convert signup_date to Date
customer_df = customer_df.withColumn(
    "signup_date",
    to_date(col("signup_date"), "dd-MM-yy")
)

# Fill missing values
customer_df = customer_df.fillna({
    "email": "unknown@example.com",
    "gender": "Unknown",
    "region": "Unknown",
    "loyalty_tier": "Bronze"
})

# Display cleaned data
display(customer_df)

# Verify schema
customer_df.printSchema()

# Verify gender values
customer_df.select("gender").distinct().show()

# Check remaining nulls
customer_df.select(
    [count(when(col(c).isNull(), c)).alias(c) for c in customer_df.columns]
).show()
customer_df.write \
    .format("delta") \
    .mode("overwrite") \
    .save("abfss://retail@retailetlstorage12345.dfs.core.windows.net/silver/customer")
silver_customer_df = spark.read \
    .format("delta") \
    .load("abfss://retail@retailetlstorage12345.dfs.core.windows.net/silver/customer")

display(silver_customer_df)