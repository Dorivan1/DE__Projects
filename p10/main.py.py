from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import pandas as pd



spark = SparkSession.builder.appName("MyApp").master("local[2]") .config("spark.executor.memory", "2g")\
    .config("spark.hadoop.home.dir", "C:\\Users\\gudex\\cs\\winutils\\winutils-master")\
    .config("spark.executorEnv.HADOOP_HOME", "C:\\Users\\gudex\\cs\\winutils\\winutils-master\\bin")\
    .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
    .getOrCreate()
df = spark.read.csv("sales_data_sample.csv", header = False)
df.dropDuplicates()
spark.sparkContext.setLogLevel("ERROR")

total_customers = df.select(F.countDistinct("_c14").alias("TotalCustomers"))
total_customers.show()


average_transaction_amount = df.groupBy("_c13").agg(F.avg("_c4").alias("AverageTransactionAmount"))
average_transaction_amount = average_transaction_amount.withColumnRenamed("_c13", "CUSTOMERNAME  ")
average_transaction_amount.show()



transaction_frequencies = df.groupBy("_c13").count().orderBy("count", ascending=False)
transaction_frequencies  = transaction_frequencies .withColumnRenamed("_c13", "CUSTOMERNAME  ")
transaction_frequencies.show()



total_spending_per_customer = df.groupBy("_c13").agg(F.sum("_c4").alias("TotalSpending"))
high_value_customers = total_spending_per_customer.orderBy("TotalSpending", ascending=False)
high_value_customers = high_value_customers.withColumnRenamed("_c13", "CUSTOMERNAME  ")
high_value_customers.show()



transaction_patterns = df.groupBy("_c9", "_c8").agg(F.sum("_c4").alias("TotalSales")).orderBy("_c9", "_c8")
transaction_patterns = transaction_patterns.withColumnRenamed("_c9", "YEAR_ID")
transaction_patterns = transaction_patterns.withColumnRenamed("_c8", "MONTH_ID")
transaction_patterns.show()


# Determine popular products based on the frequency of purchase and analyze revenue trends over time
popular_products = df.groupBy("_c10").count().orderBy("count", ascending=False)
revenue_trends = df.groupBy("_c10", "_c9", "_c8").agg(F.sum("_c4").alias("TotalSales")).orderBy("_c10", "_c9", "_c8")

popular_products = popular_products.withColumnRenamed("_c10", "PRODUCTLINE  ")

revenue_trends = revenue_trends.withColumnRenamed("_c10", "PRODUCTLINE ")
revenue_trends = revenue_trends.withColumnRenamed("_c9", "YEAR_ID  ")
revenue_trends = revenue_trends.withColumnRenamed("_c8", "MONTH_ID   ")
popular_products.show()
revenue_trends.show()


# Save total_customers DataFrame as CSV
# Convert each Spark DataFrame to a Pandas DataFrame
total_customers_pd = total_customers.toPandas()
average_transaction_amount_pd = average_transaction_amount.toPandas()
transaction_frequencies_pd = transaction_frequencies.toPandas()
high_value_customers_pd = high_value_customers.toPandas()
transaction_patterns_pd = transaction_patterns.toPandas()
popular_products_pd = popular_products.toPandas()
revenue_trends_pd = revenue_trends.toPandas()

# Write each Pandas DataFrame to a CSV file
total_customers_pd.to_csv('total_customers.csv', mode='w', header=True, index=False)
average_transaction_amount_pd.to_csv('average_transaction_amount.csv', mode='w', header=True, index=False)
transaction_frequencies_pd.to_csv('transaction_frequencies.csv', mode='w', header=True, index=False)
high_value_customers_pd.to_csv('high_value_customers.csv', mode='w', header=True, index=False)
transaction_patterns_pd.to_csv('transaction_patterns.csv', mode='w', header=True, index=False)
popular_products_pd.to_csv('popular_products.csv', mode='w', header=True, index=False)
revenue_trends_pd.to_csv('revenue_trends.csv', mode='w', header=True, index=False)
