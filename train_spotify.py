from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler, StringIndexer, StandardScaler, Imputer
from pyspark.ml.recommendation import ALS
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.clustering import KMeans
from pyspark.ml import Pipeline
from pyspark.sql.functions import col, rand, abs, hash, when 
from pyspark.sql.types import DoubleType, IntegerType 

spark = SparkSession.builder \
    .appName("SpotifyModelsTraining") \
    .config("spark.driver.memory", "8g") \
    .config("spark.executor.memory", "8g") \
    .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
    .getOrCreate()

schema = """
    id INT, artist_name STRING, track_name STRING, track_id STRING, popularity STRING, year STRING, genre STRING,
    danceability DOUBLE, energy DOUBLE, key INT, loudness DOUBLE, mode INT, speechiness DOUBLE,
    acousticness DOUBLE, instrumentalness DOUBLE, liveness DOUBLE, valence DOUBLE, tempo DOUBLE,
    duration_ms INT, time_signature INT
"""

print("Reading batch data...")
full_data = spark.read.csv("batch_*.csv", header=True, schema=schema, multiLine=True, escape="\"")
full_data = full_data.withColumn("popularity_numeric", col("popularity").cast(DoubleType())) \
                     .filter(col("popularity_numeric").isNotNull())

# feature col 
feature_columns = [
    'danceability', 'energy', 'loudness', 'speechiness',
    'acousticness', 'instrumentalness', 'liveness', 'valence', 'tempo'
]

imputer = Imputer(
    inputCols=feature_columns,
    outputCols=[f"{c}_imputed" for c in feature_columns],
    strategy="mean" # atau median
)
full_data = imputer.fit(full_data).transform(full_data)
imputed_feature_columns = [f"{c}_imputed" for c in feature_columns]


# ALS
print("Training model...")
dummy_ratings = full_data.select(
    (abs(hash(col("track_id"))) % 100000).alias("user_id").cast(IntegerType()),
    abs(hash(col("track_id"))).alias("item_id_int").cast(IntegerType()), 
    ((col("popularity_numeric") / 20.0) + (rand() * 0.5)).alias("rating") 
).filter(col("rating").isNotNull() & (~col("rating").isin([float('nan'), float('inf'), float('-inf')]))) \
 .cache()

print(f"Number of ratings for ALS: {dummy_ratings.count()}")
dummy_ratings.show(5, truncate=False)

if dummy_ratings.isEmpty():
    print("ERROR: No data available for ALS training after filtering. Check data cleaning steps.")
else:
    als = ALS(
        maxIter=10,
        regParam=0.1,
        userCol="user_id",
        itemCol="item_id_int", 
        ratingCol="rating",
        coldStartStrategy="drop",
        nonnegative=True 
    )
    als_model = als.fit(dummy_ratings)
    als_model.write().overwrite().save("als_model_v2") 
    print("✅ Recommendation model trained and saved as als_model_v2")


# Random Forest (klasifikasi)
print("Training model...")
genre_indexer = StringIndexer(inputCol="genre", outputCol="genre_label", handleInvalid="keep")
assembler_rf = VectorAssembler(inputCols=imputed_feature_columns, outputCol="features_rf")

rf = RandomForestClassifier(
    labelCol="genre_label",
    featuresCol="features_rf",
    numTrees=50,
    maxDepth=5,
    seed=42
)

pipeline_rf = Pipeline(stages=[
    genre_indexer,
    assembler_rf,
    rf
])

rf_model = pipeline_rf.fit(full_data)
rf_model.write().overwrite().save("rf_model_v2")
print("✅ Genre classification model trained and saved as rf_model_v2")


# K-Means (clustering)
print("Training model...")
assembler_km = VectorAssembler(inputCols=imputed_feature_columns, outputCol="features_km")
scaler = StandardScaler(inputCol="features_km", outputCol="scaled_features_km")
kmeans = KMeans(k=2, seed=42, featuresCol="scaled_features_km")

pipeline_kmeans = Pipeline(stages=[
    assembler_km,
    scaler,
    kmeans
])

kmeans_model = pipeline_kmeans.fit(full_data)
kmeans_model.write().overwrite().save("kmeans_model_v2")
print("✅ Clustering model trained and saved as kmeans_model_v2")

api_data = full_data.withColumn("item_id_int", abs(hash(col("track_id"))).cast(IntegerType())) \
                    .select("artist_name", "track_name", "track_id", "item_id_int", "genre", "popularity_numeric", *imputed_feature_columns)

api_data.write.parquet("full_data_v2.parquet", mode="overwrite")
print("✅ Full data (with item_id_int and imputed features) saved for API as full_data_v2.parquet")

spark.stop()