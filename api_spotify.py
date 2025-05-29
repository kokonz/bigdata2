from flask import Flask, request, jsonify, current_app
from pyspark.sql import SparkSession
from pyspark.ml.recommendation import ALSModel
from pyspark.ml.pipeline import PipelineModel
import pyspark.sql.functions as F 
from pyspark.sql.types import StructType, StructField, DoubleType, IntegerType
import logging

app = Flask(__name__)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# init func
def init_spark_and_models():
    """Initialize Spark and load models."""
    spark = SparkSession.builder.appName("SpotifyAPI").getOrCreate()
    
    logger.info("Loading models and data...")
    try:
        als_model = ALSModel.load("als_model_v2")
        rf_model = PipelineModel.load("rf_model_v2")
        kmeans_model = PipelineModel.load("kmeans_model_v2")
        full_data_api = spark.read.parquet("full_data_v2.parquet").cache()
        
        logger.info("Full Data API Schema:")
        full_data_api.printSchema()
        
        genre_indexer_model = rf_model.stages[0]
        genre_mapping_from_model = genre_indexer_model.labels
        
        logger.info(f"Number of rows in full_data_api: {full_data_api.count()}")
        logger.info("✅ Models and data loaded successfully")
        
        return spark, als_model, rf_model, kmeans_model, full_data_api, genre_mapping_from_model
    except Exception as e:
        logger.critical(f"FATAL: Could not load models or data: {e}", exc_info=True)
        raise

try:
    SPARK_SESSION, ALS_MODEL, RF_MODEL, KMEANS_MODEL, FULL_DATA_API, GENRE_MAPPING = init_spark_and_models()
except Exception as e:
    logger.critical("Application startup failed due to model loading error. Exiting.")
    exit(1) 


FEATURE_COLUMNS_API = [
    'danceability_imputed', 'energy_imputed', 'loudness_imputed', 'speechiness_imputed',
    'acousticness_imputed', 'instrumentalness_imputed', 'liveness_imputed', 'valence_imputed', 'tempo_imputed'
]

def _get_features_from_request(json_data, feature_names_expected_by_model):
    feature_values = []
    for model_feature_name in feature_names_expected_by_model:
        request_feature_name = model_feature_name.replace('_imputed', '')
        feature_values.append(float(json_data.get(request_feature_name, 0.0))) 
    return feature_values

@app.route('/recommend', methods=['POST'])
def recommend():
    try:
        data = request.json
        liked_track_ids_str = data.get('liked_songs', []) 
        
        if not liked_track_ids_str:
            return jsonify({"error": "No 'liked_songs' provided or list is empty"}), 400
        
        first_liked_track_id_str = liked_track_ids_str[0]
        
        py_hash = hash(first_liked_track_id_str) 
        py_abs_hash = abs(py_hash) 
        user_id_for_rec = py_abs_hash % 100000
        
        user_schema = StructType([
            StructField("user_id", IntegerType(), True) 
        ])
        user_df = SPARK_SESSION.createDataFrame([(user_id_for_rec,)], schema=user_schema)
        
        recommendations_df = ALS_MODEL.recommendForUserSubset(user_df, 10)
        
        if recommendations_df.isEmpty():
            return jsonify({"recommendations": [], "message": "No recommendations found for this user."})
            
        rec_list_struct = recommendations_df.select("recommendations").collect()[0]
        
        if not rec_list_struct or not rec_list_struct.recommendations:
             return jsonify({"recommendations": [], "message": "No recommendation items found."})

        rec_item_ids_int = [rec.item_id_int for rec in rec_list_struct.recommendations]

        recommended_songs_details = (FULL_DATA_API
            .filter(F.col("item_id_int").isin(rec_item_ids_int))
            .select("track_id", "track_name", "artist_name", "genre")
            .distinct()
            .collect()
        )
        
        rec_details_list = [row.asDict() for row in recommended_songs_details]
        
        return jsonify({"recommendations": rec_details_list})
        
    except Exception as e:
        logger.error(f"Error in /recommend: {str(e)}", exc_info=True)
        return jsonify({"error": "An internal server error occurred while generating recommendations."}), 500

@app.route('/predict-genre', methods=['POST'])
def predict_genre():
    """Endpoint 2: Prediksi genre berdasarkan fitur audio"""
    try:
        data = request.json
        input_features = _get_features_from_request(data, FEATURE_COLUMNS_API)
        
        schema_fields = [StructField(name, DoubleType(), True) for name in FEATURE_COLUMNS_API]
        input_df_schema = StructType(schema_fields)
        
        df_for_prediction = SPARK_SESSION.createDataFrame([input_features], schema=input_df_schema)
        
        prediction_result_df = RF_MODEL.transform(df_for_prediction)
        predicted_label_numeric = prediction_result_df.select("prediction").collect()[0][0]
        
        genre_map = GENRE_MAPPING
        predicted_genre_str = genre_map[int(predicted_label_numeric)] if 0 <= int(predicted_label_numeric) < len(genre_map) else "unknown_genre_label"
        
        return jsonify({"predicted_genre": predicted_genre_str})
        
    except Exception as e:
        logger.error(f"Error in /predict-genre: {str(e)}", exc_info=True)
        return jsonify({"error": "An internal server error occurred while predicting genre."}), 500

@app.route('/cluster-song', methods=['POST'])
def cluster_song():
    """Endpoint 3: Clustering lagu berdasarkan fitur audio"""
    try:
        data = request.json
        input_features = _get_features_from_request(data, FEATURE_COLUMNS_API)

        schema_fields = [StructField(name, DoubleType(), True) for name in FEATURE_COLUMNS_API]
        input_df_schema = StructType(schema_fields)
        
        df_for_clustering = SPARK_SESSION.createDataFrame([input_features], schema=input_df_schema)
        
        prediction_result_df = KMEANS_MODEL.transform(df_for_clustering)
        cluster_id = prediction_result_df.select("prediction").collect()[0][0]
        
        cluster_descriptions = {
            0: "Cluster 0: Potentially high energy, danceable",
            1: "Cluster 1: Potentially calmer, acoustic or instrumental",
        }
        
        return jsonify({
            "cluster_id": int(cluster_id),
            "description": cluster_descriptions.get(int(cluster_id), "Unknown_Cluster_ID")
        })
        
    except Exception as e:
        logger.error(f"Error in /cluster-song: {str(e)}", exc_info=True)
        return jsonify({"error": "An internal server error occurred while clustering song."}), 500

if __name__ == '__main__':
    logger.info("Starting Flask development server...")
    app.run(host='0.0.0.0', port=5000, debug=True)