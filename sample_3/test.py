from pyspark.sql import DataFrame
from pyspark.sql.functions import lit
from delta import *
import pyspark


builder = pyspark.sql.SparkSession.builder.appName("MyApp") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = configure_spark_with_delta_pip(builder).getOrCreate()

# Create a sample DataFrame

data = [("Alice", 34), ("Bob", 45), ("Cathy", 29)]

columns = ["Name", "Age"]

df: DataFrame = spark.createDataFrame(data, columns)

 

# Save the DataFrame into a Delta table

df.write.format("delta").mode("overwrite").saveAsTable("sample_delta_table")