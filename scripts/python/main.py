
import findspark; findspark.init()
from pyspark.sql import SparkSession
from pyspark.sql import functions as f

if __name__ == "__main__":
    spark = SparkSession.builder.master("local[*]").appName("PySparkShell").getOrCreate()

    raw = spark.sparkContext.wholeTextFiles("data/*.txt")

    books = raw.toDF(["path", "text"])\
        .withColumn("file_name", f.regexp_extract("path",r"([^/]+)$",1))\
        .select("file_name", "text")
    print(books.count())
    spark.stop()
