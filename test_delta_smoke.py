from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

builder = (
    SparkSession.builder
    .appName("bronze-ingestion-test")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
)

spark = configure_spark_with_delta_pip(builder).getOrCreate()

df = spark.createDataFrame([(1, "test")], ["id", "value"])
df.write.format("delta").mode("overwrite").save("/tmp/delta-smoke-test")

spark.read.format("delta").load("/tmp/delta-smoke-test").show()
