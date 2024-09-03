from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, when, lit
from pyspark.sql.types import DoubleType
from config import DATABASE

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Data Cleaning and Standardization") \
    .getOrCreate()

# Load CSV files into DataFrames
sales_df = spark.read.csv("sales_data.csv", header=True)
products_df = spark.read.csv("db_products.csv", header=True)
transactions_df = spark.read.csv("db_transactions.csv", header=True)

default_sales_values = {
    "Transaction_ID": "UNKNOWN",
    "Product_ID": "UNKNOWN",
    "Quantity": 0,
    "Price": 0.0,
    "Transaction_Date": "1970-01-01"
}

default_products_values = {
    "product_id": "UNKNOWN",
    "product_name": "UNKNOWN",
    "category": "UNKNOWN",
    "price": 0.0,
    "stock_available": 0
}

default_transactions_values = {
    "transaction_id": "UNKNOWN",
    "customer_id": "UNKNOWN",
    "product_id": "UNKNOWN",
    "quantity": 0,
    "transaction_date": "1970-01-01",
    "total_amount": 0.0
}

sales_df = sales_df.fillna(default_sales_values)
products_df = products_df.fillna(default_products_values)
transactions_df = transactions_df.fillna(default_transactions_values)

# Handle duplicate records
sales_df = sales_df.dropDuplicates()
products_df = products_df.dropDuplicates()
transactions_df = transactions_df.dropDuplicates()

# Convert dates to consistent format
sales_df = sales_df.withColumn("Transaction_Date", 
                               to_date(col("Transaction_Date"), "MM/dd/yyyy"))
transactions_df = transactions_df.withColumn("transaction_date", 
                                             to_date(col("transaction_date"), "yyyy-MM-dd"))

# Correct inconsistent data entries
# Handling abnormal values
sales_df = sales_df.withColumn("Price", 
                               when(col("Price") < 0, None).otherwise(col("Price")))

products_df = products_df.withColumn("price", 
                                     when(col("price") < 0, None).otherwise(col("price")))

transactions_df = transactions_df.withColumn("total_amount", 
                                             when(col("total_amount") < 0, None).otherwise(col("total_amount")))

# Convert price columns to DoubleType
sales_df = sales_df.withColumn("Price", col("Price").cast(DoubleType()))
products_df = products_df.withColumn("price", col("price").cast(DoubleType()))
transactions_df = transactions_df.withColumn("total_amount", col("total_amount").cast(DoubleType()))

# Fill nulls in quantities and prices with 0 or mean (optional)
sales_df = sales_df.fillna({"Quantity": 0, "Price": 0})
products_df = products_df.fillna({"price": 0, "stock_available": 0})
transactions_df = transactions_df.fillna({"quantity": 0, "total_amount": 0})

# Rename columns for consistency
sales_df = sales_df.withColumnRenamed('Product_ID', 'product_id')
products_df = products_df.withColumnRenamed('product_id', 'product_id')
transactions_df = transactions_df.withColumnRenamed('product_id', 'product_id')

# Ensure 'product_id' is of type string and trim any spaces
sales_df = sales_df.withColumn('product_id', col('product_id').cast('int'))
products_df = products_df.withColumn('product_id', col('product_id').cast('int'))
transactions_df = transactions_df.withColumn('product_id', col('product_id').cast('int'))

# Join sales_df with products_df on 'product_id'
sales_with_products_df = sales_df.join(products_df, on='product_id', how='left')

# Join the result with transactions_df on 'product_id'
merged_df = sales_with_products_df.join(transactions_df, on='product_id', how='left')

# print('merged data')
# merged_data.show(5)


# Save the data as SQL tables
merged_df.write.format("jdbc").options(
    url="jdbc:mysql://localhost:3306/mydatabase",
    driver="com.mysql.jdbc.Driver",
    dbtable=DATABASE['db_name'],
    user=DATABASE["root"],
    password=DATABASE["password"]
).mode("overwrite").save()

