from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

builder = (
    SparkSession.builder
    .appName("s3a-localstack-smoke-test")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:4566")
    .config("spark.hadoop.fs.s3a.access.key", "test")
    .config("spark.hadoop.fs.s3a.secret.key", "test") # LocalStack accepts any string as credentials — not a real secret
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
)

spark = configure_spark_with_delta_pip(
    builder,
    extra_packages=["org.apache.hadoop:hadoop-aws:3.3.4"],
).getOrCreate()

df = spark.createDataFrame([(1, "test")], ["id", "value"])
df.write.format("delta").mode("overwrite").save("s3a://nyc-taxi-pipeline-local/bronze/smoke-test")

spark.read.format("delta").load("s3a://nyc-taxi-pipeline-local/bronze/smoke-test").show()
