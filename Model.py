# Initialize Spark with Delta Lake support and optimized settings
spark = SparkSession.builder \
    .appName("MovieRecommendation") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \  # Delta Lake extension
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \  # Unified catalog
    .config("spark.executor.memory", "8g") \  # Memory allocation
    .config("spark.driver.memory", "4g") \
    .getOrCreate()


# Load raw data and convert to Delta Lake format
def load_data(raw_path, delta_path):
    """
    Args:
        raw_path: Source CSV path 
        delta_path: Target Delta table path
    Returns:
        Delta table reference
    """
    df = spark.read.format("csv").option("header", "true").load(raw_path)
    df.write.format("delta").mode("overwrite").save(delta_path)  # Write as Delta
    return spark.read.format("delta").load(delta_path)  # Return Delta reader

# Usage:
ratings = load_data("s3a://data/ratings.csv", "s3a://delta/ratings")


from pyspark.ml.tuning import ParamGridBuilder, CrossValidator

# Define ALS model
als = ALS(
    userCol="userId",
    itemCol="movieId",
    ratingCol="rating",
    coldStartStrategy="drop",  # Handle missing data
    nonnegative=True  # Force positive factors
)

# Parameter grid for tuning
param_grid = ParamGridBuilder() \
    .addGrid(als.rank, [10, 50, 100]) \  # Latent factors
    .addGrid(als.regParam, [0.01, 0.1, 1.0]) \  # Regularization
    .build()

# Cross-validation setup
cv = CrossValidator(
    estimator=als,
    estimatorParamMaps=param_grid,
    evaluator=RegressionEvaluator(metricName="rmse"),
    numFolds=3,  # 3-fold validation
    parallelism=4  # Parallel jobs
)

# Train model
model = cv.fit(training_data)

# Time travel to previous version
historical_recs = spark.read.format("delta") \
    .option("versionAsOf", 12) \  # Get version 12
    .load("s3a://delta/recommendations")

# Schema evolution
spark.sql("""
ALTER TABLE ratings 
ADD COLUMNS (device_type STRING COMMENT 'Mobile/Web')
""")

# Optimize storage
spark.sql("OPTIMIZE ratings ZORDER BY (userId, movieId)")  # Co-locate user data

from pyspark.sql.functions import udf
from pyspark.sql.types import FloatType

# Content-based similarity UDF
@udf(FloatType())
def genre_similarity(genres1, genres2):
    """Cosine similarity between genre vectors"""
    intersection = set(genres1).intersection(set(genres2))
    return float(len(intersection)) / (len(genres1) * len(genres2)) ** 0.5

# Blend ALS and content scores
final_recs = als_predictions.withColumn(
    "hybrid_score", 
    0.7 * col("als_prediction") + 0.3 * genre_similarity(col("user_genres"), col("movie_genres"))
)

# Read from Kafka
stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "host1:port1") \
    .option("subscribe", "ratings_topic") \
    .load()

# Write to Delta Lake with merge
stream.writeStream \
    .format("delta") \
    .option("checkpointLocation", "/checkpoints") \
    .trigger(processingTime="10 seconds") \  # Micro-batch
    .foreachBatch(lambda df, batch_id: 
        df.write.format("delta").mode("append").save("/delta/ratings_stream"))
    .start()
