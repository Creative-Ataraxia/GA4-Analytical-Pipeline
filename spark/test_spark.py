from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("minio-ga4-check")
    .master("spark://spark-master:7077")
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
    .config("spark.hadoop.fs.s3a.access.key", "minio")
    .config("spark.hadoop.fs.s3a.secret.key", "miniopass")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .config("spark.driver.extraJavaOptions", "-Dhadoop.security.authentication=simple")
    .config("spark.executor.extraJavaOptions", "-Dhadoop.security.authentication=simple")
    .getOrCreate()
)

df = spark.read.parquet("s3a://ga4-bronze/events_20210131.parquet")
print("Row count:", df.count())
df.printSchema()
spark.stop()
