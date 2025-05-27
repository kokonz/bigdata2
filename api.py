from flask import Flask, request, jsonify
from pyspark.ml.clustering import KMeansModel
from pyspark.sql import SparkSession
import numpy as np

app = Flask(__name__)
spark = SparkSession.builder.appName("TaxiAPI").getOrCreate()

# endpoint 1 : prediksi cluster (model 1)
@app.route('/v1/cluster', methods=['POST'])
def cluster_v1():
    data = request.json
    point = [data['lon'], data['lat']]
    model = KMeansModel.load("model_v1")
    cluster = model.predict([point])
    return jsonify({"model_version": 1, "cluster": int(cluster)})

# endpoint 2 : prediksi cluster (model 2)
@app.route('/v2/cluster', methods=['POST'])
def cluster_v2():
    data = request.json
    point = [data['lon'], data['lat']]
    model = KMeansModel.load("model_v2")
    cluster = model.predict([point])
    return jsonify({"model_version": 2, "cluster": int(cluster)})

# endpoint 3 : rekomendasi area (model 3)
@app.route('/v3/recommend', methods=['GET'])
def recommend():
    model = KMeansModel.load("model_v3")
    centers = model.clusterCenters()
    areas = ["Financial District", "Midtown", "JFK Airport"] 
    return jsonify({
        "model_version": 3,
        "recommended_areas": areas
    })

if __name__ == '__main__':
    app.run(port=5000)