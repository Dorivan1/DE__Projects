from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, col

spark = SparkSession.builder \
    .appName("WordCountApp") \
    .getOrCreate()

text_file = spark.read.text("example.txt")

words = text_file.select(explode(split(col("value"), "\\s+")).alias("word"))

# Map each word to (word, 1) tuple
word_counts = words.groupBy("word").count()

# Output the result
print("Word Counts:")
word_counts.show(truncate=False)

# Stop SparkSession
spark.stop()


